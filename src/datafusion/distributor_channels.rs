// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Special channel construction to distribute data from various inputs into N outputs
//! minimizing buffering but preventing deadlocks when repartitioning.
//!
//! Copied from DataFusion's `datafusion-physical-plan/src/repartition/distributor_channels.rs`
//! to solve the JOIN deadlock problem where a single parser produces multiple streams.
//!
//! # Design
//!
//! ```text
//! +----+      +------+
//! | TX |==||  | Gate |
//! +----+  ||  |      |  +--------+  +----+
//!         ====|      |==| Buffer |==| RX |
//! +----+  ||  |      |  +--------+  +----+
//! | TX |==||  |      |
//! +----+      |      |
//!             |      |
//! +----+      |      |  +--------+  +----+
//! | TX |======|      |==| Buffer |==| RX |
//! +----+      +------+  +--------+  +----+
//! ```
//!
//! There are `N` virtual MPSC (multi-producer, single consumer) channels with unbounded capacity.
//! However, if all buffers/channels are non-empty, then a global gate will be closed preventing
//! new data from being written (the sender futures will be [pending](Poll::Pending)) until at
//! least one channel is empty (and not closed).
//!
//! # Backpressure Strategy
//!
//! The gate provides **global backpressure**: sends only block when ALL channels are non-empty.
//! This guarantees forward progress as long as ANY consumer is actively draining its channel.
//!
//! This design intentionally avoids per-channel backpressure (e.g., high-water marks per channel)
//! because per-channel limits can cause deadlock when:
//! - A single producer sends to multiple channels (e.g., parser → multiple entity types)
//! - Consumers pull from channels at different rates (e.g., UNION ALL with CoalescePartitionsExec)
//! - A fast-filling channel blocks the producer, starving slow-filling channels
//!
//! The tradeoff: memory usage can spike if one consumer is much faster than others, since
//! fast-draining channels allow the producer to keep sending to slow-draining channels.
//!
//! # Future Enhancement: Global Memory Limit
//!
//! If memory becomes a concern, a potential enhancement would be to track total items
//! across ALL channels and apply global backpressure when the total exceeds a threshold.
//! This would bound memory while preserving the deadlock-freedom guarantee:
//! - Track: `total_items` across all channels
//! - Block sends when: `total_items >= GLOBAL_LIMIT`
//! - Resume when: `total_items < GLOBAL_LIMIT`
//! This is NOT currently implemented but documented here as a potential future direction.

use std::{
    collections::VecDeque,
    future::Future,
    ops::DerefMut,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

/// Create `n` empty channels.
pub fn channels<T>(n: usize) -> (Vec<DistributionSender<T>>, Vec<DistributionReceiver<T>>) {
    let channels = (0..n)
        .map(|id| Arc::new(Channel::new_with_one_sender(id)))
        .collect::<Vec<_>>();
    let gate = Arc::new(Gate {
        empty_channels: AtomicUsize::new(n),
        send_wakers: Mutex::new(None),
    });
    let senders = channels
        .iter()
        .map(|channel| DistributionSender {
            channel: Arc::clone(channel),
            gate: Arc::clone(&gate),
        })
        .collect();
    let receivers = channels
        .into_iter()
        .map(|channel| DistributionReceiver {
            channel,
            gate: Arc::clone(&gate),
        })
        .collect();
    (senders, receivers)
}

/// Error during [send](DistributionSender::send).
///
/// This occurs when the [receiver](DistributionReceiver) is gone.
#[derive(PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SendError").finish()
    }
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot send data, receiver is gone")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Sender side of distribution [channels].
///
/// This handle can be cloned. All clones will write into the same channel. Dropping the last
/// sender will close the channel. In this case, the [receiver](DistributionReceiver) will still
/// be able to poll the remaining data, but will receive `None` afterwards.
#[derive(Debug)]
pub struct DistributionSender<T> {
    channel: SharedChannel<T>,
    gate: SharedGate,
}

impl<T> DistributionSender<T> {
    /// Send data.
    ///
    /// This fails if the [receiver](DistributionReceiver) is gone.
    pub fn send(&self, element: T) -> SendFuture<'_, T> {
        SendFuture {
            channel: &self.channel,
            gate: &self.gate,
            element: Box::new(Some(element)),
        }
    }

    /// Returns true if the global gate is currently blocking sends.
    ///
    /// When true, all channels are non-empty and producers are waiting
    /// for consumers to drain at least one channel.
    pub fn is_gate_blocked(&self) -> bool {
        self.gate.empty_channels.load(Ordering::SeqCst) == 0
    }

    /// Returns the number of senders currently blocked by the gate.
    pub fn blocked_sender_count(&self) -> usize {
        self.gate
            .send_wakers
            .lock()
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

impl<T> Clone for DistributionSender<T> {
    fn clone(&self) -> Self {
        self.channel.n_senders.fetch_add(1, Ordering::SeqCst);

        Self {
            channel: Arc::clone(&self.channel),
            gate: Arc::clone(&self.gate),
        }
    }
}

impl<T> Drop for DistributionSender<T> {
    fn drop(&mut self) {
        let n_senders_pre = self.channel.n_senders.fetch_sub(1, Ordering::SeqCst);
        if n_senders_pre > 1 {
            return;
        }

        let receivers = {
            let mut state = self.channel.state.lock();

            if state
                .data
                .as_ref()
                .map(|data| data.is_empty())
                .unwrap_or_default()
            {
                self.gate.decr_empty_channels();
            }

            state.recv_wakers.take().expect("not closed yet")
        };

        for recv in receivers {
            recv.wake();
        }
    }
}

/// Future backing [send](DistributionSender::send).
#[derive(Debug)]
pub struct SendFuture<'a, T> {
    channel: &'a SharedChannel<T>,
    gate: &'a SharedGate,
    element: Box<Option<T>>,
}

impl<T> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(this.element.is_some(), "polled ready future");

        let to_wake = {
            let mut guard_channel_state = this.channel.state.lock();
            let channel_state = guard_channel_state.deref_mut();

            let Some(data) = channel_state.data.as_mut() else {
                return Poll::Ready(Err(SendError(this.element.take().expect("just checked"))));
            };

            // Gate check: are ALL channels non-empty?
            // If so, block to prevent unbounded memory growth.
            // This guarantees forward progress: as long as ANY consumer is draining,
            // the gate will eventually open and allow sends to continue.
            if this.gate.empty_channels.load(Ordering::SeqCst) == 0 {
                let mut guard = this.gate.send_wakers.lock();
                if let Some(send_wakers) = guard.deref_mut() {
                    send_wakers.push((cx.waker().clone(), this.channel.id));
                    return Poll::Pending;
                }
            }

            let was_empty = data.is_empty();
            data.push_back(this.element.take().expect("just checked"));

            if was_empty {
                this.gate.decr_empty_channels();
                channel_state.take_recv_wakers()
            } else {
                Vec::with_capacity(0)
            }
        };

        for receiver in to_wake {
            receiver.wake();
        }

        Poll::Ready(Ok(()))
    }
}

/// Receiver side of distribution [channels].
#[derive(Debug)]
pub struct DistributionReceiver<T> {
    channel: SharedChannel<T>,
    gate: SharedGate,
}

impl<T> DistributionReceiver<T> {
    /// Receive data from channel.
    ///
    /// Returns `None` if the channel is empty and no [senders](DistributionSender) are left.
    pub fn recv(&mut self) -> RecvFuture<'_, T> {
        RecvFuture {
            channel: &mut self.channel,
            gate: &mut self.gate,
            rdy: false,
        }
    }

    /// Returns the number of items currently buffered in this channel.
    pub fn queue_len(&self) -> usize {
        self.channel
            .state
            .lock()
            .data
            .as_ref()
            .map(|d| d.len())
            .unwrap_or(0)
    }
}

impl<T> Drop for DistributionReceiver<T> {
    fn drop(&mut self) {
        let mut guard_channel_state = self.channel.state.lock();
        let data = guard_channel_state.data.take().expect("not dropped yet");

        if data.is_empty() && (self.channel.n_senders.load(Ordering::SeqCst) > 0) {
            self.gate.decr_empty_channels();
        }

        self.gate.wake_channel_senders(self.channel.id);
    }
}

/// Future backing [recv](DistributionReceiver::recv).
pub struct RecvFuture<'a, T> {
    channel: &'a mut SharedChannel<T>,
    gate: &'a mut SharedGate,
    rdy: bool,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(!this.rdy, "polled ready future");

        let mut guard_channel_state = this.channel.state.lock();
        let channel_state = guard_channel_state.deref_mut();

        // Pop from the buffer
        let data = channel_state.data.as_mut().expect("not dropped yet");
        let element = data.pop_front();
        let is_empty = data.is_empty();

        match element {
            Some(element) => {
                // Check if channel became empty - need to update gate
                let gate_wakers = if is_empty && channel_state.recv_wakers.is_some() {
                    let old_counter = this.gate.empty_channels.fetch_add(1, Ordering::SeqCst);

                    if old_counter == 0 {
                        let mut guard = this.gate.send_wakers.lock();

                        if this.gate.empty_channels.load(Ordering::SeqCst) > 0 {
                            guard.take().unwrap_or_default()
                        } else {
                            Vec::with_capacity(0)
                        }
                    } else {
                        Vec::with_capacity(0)
                    }
                } else {
                    Vec::with_capacity(0)
                };

                drop(guard_channel_state);

                // Wake gate-blocked senders (any channel)
                for (waker, _channel_id) in gate_wakers {
                    waker.wake();
                }

                this.rdy = true;
                Poll::Ready(Some(element))
            }
            None => {
                if let Some(recv_wakers) = channel_state.recv_wakers.as_mut() {
                    recv_wakers.push(cx.waker().clone());
                    Poll::Pending
                } else {
                    this.rdy = true;
                    Poll::Ready(None)
                }
            }
        }
    }
}

#[derive(Debug)]
struct Channel<T> {
    n_senders: AtomicUsize,
    id: usize,
    state: Mutex<ChannelState<T>>,
}

impl<T> Channel<T> {
    fn new_with_one_sender(id: usize) -> Self {
        Channel {
            n_senders: AtomicUsize::new(1),
            id,
            state: Mutex::new(ChannelState {
                data: Some(VecDeque::default()),
                recv_wakers: Some(Vec::default()),
            }),
        }
    }
}

#[derive(Debug)]
struct ChannelState<T> {
    data: Option<VecDeque<T>>,
    recv_wakers: Option<Vec<Waker>>,
}

impl<T> ChannelState<T> {
    fn take_recv_wakers(&mut self) -> Vec<Waker> {
        let to_wake = self.recv_wakers.as_mut().expect("not closed");
        let mut tmp = Vec::with_capacity(to_wake.capacity());
        std::mem::swap(to_wake, &mut tmp);
        tmp
    }
}

type SharedChannel<T> = Arc<Channel<T>>;

#[derive(Debug)]
struct Gate {
    empty_channels: AtomicUsize,
    send_wakers: Mutex<Option<Vec<(Waker, usize)>>>,
}

impl Gate {
    fn wake_channel_senders(&self, id: usize) {
        let to_wake = {
            let mut guard = self.send_wakers.lock();

            if let Some(send_wakers) = guard.deref_mut() {
                let (wake, keep) = send_wakers.drain(..).partition(|(_waker, id2)| id == *id2);

                *send_wakers = keep;

                wake
            } else {
                Vec::with_capacity(0)
            }
        };

        for (waker, _id) in to_wake {
            waker.wake();
        }
    }

    fn decr_empty_channels(&self) {
        let old_count = self.empty_channels.fetch_sub(1, Ordering::SeqCst);

        if old_count == 1 {
            let mut guard = self.send_wakers.lock();

            if self.empty_channels.load(Ordering::SeqCst) == 0 && guard.is_none() {
                *guard = Some(Vec::new());
            }
        }
    }
}

type SharedGate = Arc<Gate>;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use futures::{task::ArcWake, FutureExt};

    use super::*;

    #[test]
    fn test_single_channel_no_gate() {
        let (txs, mut rxs) = channels(2);

        let mut recv_fut = rxs[0].recv();
        let waker = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("foo")).unwrap();
        assert!(waker.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("foo"));

        poll_ready(&mut txs[0].send("bar")).unwrap();
        poll_ready(&mut txs[0].send("baz")).unwrap();
        poll_ready(&mut txs[0].send("end")).unwrap();
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"));
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("baz"));

        drop(txs);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("end"));
        assert_eq!(poll_ready(&mut rxs[0].recv()), None);
    }

    #[test]
    fn test_gate() {
        let (txs, mut rxs) = channels(2);

        poll_ready(&mut txs[0].send("0_a")).unwrap();
        poll_ready(&mut txs[0].send("0_b")).unwrap();
        poll_ready(&mut txs[1].send("1_a")).unwrap();

        let mut send_fut = txs[1].send("1_b");
        let waker = poll_pending(&mut send_fut);

        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("0_a"));
        poll_pending(&mut send_fut);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("0_b"));

        assert!(waker.woken());
        poll_ready(&mut send_fut).unwrap();
    }

    #[test]
    fn test_gate_opens_when_any_channel_drains() {
        // Test that gate opens when ANY channel becomes empty,
        // allowing sends to ALL channels to proceed
        let (txs, mut rxs) = channels(3);

        // Fill all channels with one item each
        poll_ready(&mut txs[0].send("0_a")).unwrap();
        poll_ready(&mut txs[1].send("1_a")).unwrap();
        poll_ready(&mut txs[2].send("2_a")).unwrap();

        // Gate should now be closed (all channels non-empty)
        assert_eq!(txs[0].gate.empty_channels.load(Ordering::SeqCst), 0);

        // Try to send to channel 0 - should block
        let mut send_fut = txs[0].send("0_b");
        let waker = poll_pending(&mut send_fut);

        // Drain channel 2 (not channel 0!)
        assert_eq!(poll_ready(&mut rxs[2].recv()), Some("2_a"));

        // Gate should now be open (channel 2 is empty)
        assert_eq!(txs[0].gate.empty_channels.load(Ordering::SeqCst), 1);
        assert!(waker.woken());

        // Send to channel 0 should now succeed (even though channel 0 still has data)
        poll_ready(&mut send_fut).unwrap();
    }

    #[test]
    fn test_unbounded_growth_when_gate_open() {
        // Test that channels can grow unbounded when gate is open
        // (i.e., at least one channel is empty)
        let (txs, mut rxs) = channels(2);

        // Keep channel 0 empty, fill channel 1 with many items
        for i in 0..100 {
            poll_ready(&mut txs[1].send(format!("1_{}", i))).unwrap();
        }

        // All sends should succeed because channel 0 is empty (gate open)
        assert_eq!(txs[0].gate.empty_channels.load(Ordering::SeqCst), 1);

        // Drain channel 1 to verify all items arrived
        for i in 0..100 {
            assert_eq!(poll_ready(&mut rxs[1].recv()), Some(format!("1_{}", i)));
        }
    }

    #[track_caller]
    fn poll_ready<F>(fut: &mut F) -> F::Output
    where
        F: Future + Unpin,
    {
        match poll(fut).0 {
            Poll::Ready(x) => x,
            Poll::Pending => panic!("future is pending"),
        }
    }

    #[track_caller]
    fn poll_pending<F>(fut: &mut F) -> Arc<TestWaker>
    where
        F: Future + Unpin,
    {
        let (res, waker) = poll(fut);
        match res {
            Poll::Ready(_) => panic!("future is ready"),
            Poll::Pending => waker,
        }
    }

    fn poll<F>(fut: &mut F) -> (Poll<F::Output>, Arc<TestWaker>)
    where
        F: Future + Unpin,
    {
        let test_waker = Arc::new(TestWaker::default());
        let waker = futures::task::waker(Arc::clone(&test_waker));
        let mut cx = Context::from_waker(&waker);
        let res = fut.poll_unpin(&mut cx);
        (res, test_waker)
    }

    #[derive(Debug, Default)]
    struct TestWaker {
        woken: AtomicBool,
    }

    impl TestWaker {
        fn woken(&self) -> bool {
            self.woken.load(Ordering::SeqCst)
        }
    }

    impl ArcWake for TestWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.woken.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_queue_len_and_gate_status() {
        let (txs, rxs) = channels(2);

        // Initially both channels empty, gate open
        assert_eq!(rxs[0].queue_len(), 0);
        assert_eq!(rxs[1].queue_len(), 0);
        assert!(!txs[0].is_gate_blocked());
        assert_eq!(txs[0].blocked_sender_count(), 0);

        // Send to channel 0
        poll_ready(&mut txs[0].send("a")).unwrap();
        poll_ready(&mut txs[0].send("b")).unwrap();
        assert_eq!(rxs[0].queue_len(), 2);
        assert_eq!(rxs[1].queue_len(), 0);
        assert!(!txs[0].is_gate_blocked()); // Still open, channel 1 is empty

        // Send to channel 1 - gate should close after this
        poll_ready(&mut txs[1].send("x")).unwrap();
        assert_eq!(rxs[1].queue_len(), 1);
        assert!(txs[0].is_gate_blocked()); // All channels non-empty
    }
}
