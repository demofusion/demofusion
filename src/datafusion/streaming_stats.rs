//! Statistics for monitoring in-flight streaming data.
//!
//! Provides visibility into buffer depths, row counts, and backpressure state
//! for streaming query sessions. Useful for:
//! - Monitoring memory pressure in long-running streams
//! - Detecting slow consumers causing buffer buildup
//! - Making cancellation decisions when buffers grow too large

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Aggregate statistics for a streaming query session.
///
/// All counters are atomic and can be read concurrently while streaming is active.
/// Stats are shared across all dispatchers in a session via `Arc<StreamingStats>`.
#[derive(Debug, Default)]
pub struct StreamingStats {
    /// Total rows produced by the parser (before batching).
    pub rows_produced: AtomicU64,

    /// Total RecordBatches sent to distribution channels.
    pub batches_sent: AtomicU64,

    /// Total rows sent in batches (after batching, may differ from rows_produced
    /// if some rows are still buffered in builders).
    pub rows_sent: AtomicU64,

    /// Number of times the gate blocked sends (all channels were non-empty).
    /// High values indicate consumer backpressure.
    pub gate_blocked_count: AtomicU64,
}

impl StreamingStats {
    /// Creates a new stats tracker.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records that rows were produced by the parser.
    #[inline]
    pub fn record_rows_produced(&self, count: u64) {
        self.rows_produced.fetch_add(count, Ordering::Relaxed);
    }

    /// Records that a batch was sent with the given row count.
    #[inline]
    pub fn record_batch_sent(&self, row_count: u64) {
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
        self.rows_sent.fetch_add(row_count, Ordering::Relaxed);
    }

    /// Records that the gate blocked a send operation.
    #[inline]
    pub fn record_gate_blocked(&self) {
        self.gate_blocked_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a snapshot of current stats.
    pub fn snapshot(&self) -> StreamingStatsSnapshot {
        StreamingStatsSnapshot {
            rows_produced: self.rows_produced.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            rows_sent: self.rows_sent.load(Ordering::Relaxed),
            gate_blocked_count: self.gate_blocked_count.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of streaming statistics.
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamingStatsSnapshot {
    pub rows_produced: u64,
    pub batches_sent: u64,
    pub rows_sent: u64,
    pub gate_blocked_count: u64,
}

impl StreamingStatsSnapshot {
    /// Returns the number of rows currently buffered in batch builders
    /// (produced but not yet sent).
    pub fn rows_buffered(&self) -> u64 {
        self.rows_produced.saturating_sub(self.rows_sent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_tracking() {
        let stats = StreamingStats::new();

        stats.record_rows_produced(100);
        stats.record_rows_produced(50);
        stats.record_batch_sent(80);
        stats.record_gate_blocked();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.rows_produced, 150);
        assert_eq!(snapshot.batches_sent, 1);
        assert_eq!(snapshot.rows_sent, 80);
        assert_eq!(snapshot.gate_blocked_count, 1);
        assert_eq!(snapshot.rows_buffered(), 70);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let stats = StreamingStats::new();
        let stats_clone = Arc::clone(&stats);

        let handle = thread::spawn(move || {
            for _ in 0..1000 {
                stats_clone.record_rows_produced(1);
            }
        });

        for _ in 0..1000 {
            stats.record_rows_produced(1);
        }

        handle.join().unwrap();

        assert_eq!(stats.snapshot().rows_produced, 2000);
    }
}
