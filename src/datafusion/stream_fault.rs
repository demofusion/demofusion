//! Per-stream fault reporting between the parser and the query streams.
//!
//! A distribution channel can only carry `RecordBatch`, so when the producer
//! side dies there is nothing to distinguish "the demo ended" from "decoding
//! blew up half way through". Both look identical to the consumer: the senders
//! drop and the stream ends cleanly. That is how a parse that aborted at tick
//! 21,131 of 126,162 reported success with no rows missing that anyone could
//! see.
//!
//! [`StreamFault`] is the out-of-band signal that closes that gap. One is
//! created per distribution channel and shared between the sender side (the
//! dispatcher, plus the parser task which holds every fault) and the receiver
//! side ([`DistributionReceiverStream`]). When a fault is recorded, the stream
//! that would otherwise have ended cleanly yields a `DataFusionError` instead,
//! which travels up the physical plan into `QueryHandle` and out to the caller
//! (`failures()` in Python).
//!
//! Faults are per-channel on purpose: a batch builder failing for one table
//! must fail only that table's stream, while a parse error that corrupts the
//! shared bit stream is recorded on every live channel.
//!
//! [`DistributionReceiverStream`]: crate::datafusion::distribution_stream::DistributionReceiverStream

use std::sync::{Arc, OnceLock};

/// A write-once fault slot shared by one channel's producer and consumer.
///
/// Cloning is cheap and shares the same slot. The first recorded fault wins —
/// later ones are ignored, since the first is the one that explains the rest.
#[derive(Clone, Debug, Default)]
pub struct StreamFault(Arc<OnceLock<Arc<str>>>);

impl StreamFault {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fault. Returns true if this call is the one that set it.
    pub fn set(&self, message: impl Into<Arc<str>>) -> bool {
        self.0.set(message.into()).is_ok()
    }

    /// The recorded fault, if any.
    #[must_use]
    pub fn get(&self) -> Option<Arc<str>> {
        self.0.get().cloned()
    }

    #[must_use]
    pub fn is_set(&self) -> bool {
        self.0.get().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_fault_reads_as_none() {
        let fault = StreamFault::new();
        assert!(!fault.is_set());
        assert!(fault.get().is_none());
    }

    #[test]
    fn a_clone_observes_what_the_original_recorded() {
        let fault = StreamFault::new();
        let observer = fault.clone();

        assert!(fault.set("decode failed"));

        assert!(observer.is_set());
        assert_eq!(observer.get().as_deref(), Some("decode failed"));
    }

    #[test]
    fn the_first_fault_wins() {
        // The first failure is the one that explains every failure after it, so
        // a later write must not overwrite the diagnosis.
        let fault = StreamFault::new();

        assert!(fault.set("field path not found"));
        assert!(!fault.set("bit reader overflowed"));

        assert_eq!(fault.get().as_deref(), Some("field path not found"));
    }

    #[test]
    fn distinct_faults_do_not_leak_into_each_other() {
        // Per-channel isolation: one table's builder failing must not mark
        // another table's stream as failed.
        let a = StreamFault::new();
        let b = StreamFault::new();

        a.set("builder error");

        assert!(a.is_set());
        assert!(!b.is_set());
    }
}
