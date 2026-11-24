//! Fixed-bucket histogram built from atomics.
//!
//! Values are integers (microseconds for every duration PRISM records), so
//! recording is a bucket search plus three relaxed `fetch_add`s: no lock, no
//! allocation, no floating point. Conversion to seconds happens only when the
//! snapshot is rendered.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;

/// Bucket upper bounds for RPC latency, in microseconds: 1 ms to 60 s.
pub const RPC_LATENCY_BUCKETS_US: &[u64] = &[
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000, 10_000_000, 30_000_000, 60_000_000,
];

/// Bucket upper bounds for flush duration, in microseconds: 1 ms to 120 s.
pub const FLUSH_BUCKETS_US: &[u64] = &[
    1_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000,
    10_000_000, 30_000_000, 60_000_000, 120_000_000,
];

/// Bucket upper bounds for rate-limit waits, in microseconds: 1 ms to 60 s.
pub const RATE_LIMIT_WAIT_BUCKETS_US: &[u64] = &[
    1_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000,
    30_000_000, 60_000_000,
];

/// A histogram with fixed, inclusive upper bounds plus an implicit `+Inf`
/// bucket.
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [u64],
    /// One per bound, plus the `+Inf` bucket. Not cumulative.
    counts: Box<[AtomicU64]>,
    sum: AtomicU64,
    count: AtomicU64,
}

/// A point-in-time copy of a [`Histogram`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistogramSnapshot {
    /// `(upper bound, cumulative count)` per finite bucket, ascending. The
    /// `+Inf` bucket is `count`.
    pub buckets: Vec<(u64, u64)>,
    /// Sum of every recorded value.
    pub sum: u64,
    pub count: u64,
}

impl Histogram {
    /// `bounds` must be strictly ascending.
    pub fn new(bounds: &'static [u64]) -> Self {
        debug_assert!(bounds.windows(2).all(|w| w[0] < w[1]), "bounds must ascend");
        Self {
            bounds,
            counts: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one value. Lock-free, allocation-free.
    pub fn record(&self, value: u64) {
        let idx = self.bounds.partition_point(|&b| b < value);
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
        // Plain add: `u64` microseconds wrap only after ~584,000 years of
        // accumulated latency, not worth a CAS loop on the hot path.
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a duration as whole microseconds.
    pub fn record_duration(&self, d: Duration) {
        self.record(u64::try_from(d.as_micros()).unwrap_or(u64::MAX));
    }

    /// Copy the counts. Not an atomic cut: a value recorded concurrently may
    /// show in `count` and not yet in its bucket. The bucket totals are made
    /// consistent with `count` by deriving `count` from them.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut cumulative = 0u64;
        let mut buckets = Vec::with_capacity(self.bounds.len());
        for (i, &bound) in self.bounds.iter().enumerate() {
            cumulative += self.counts[i].load(Ordering::Relaxed);
            buckets.push((bound, cumulative));
        }
        cumulative += self.counts[self.bounds.len()].load(Ordering::Relaxed);
        HistogramSnapshot {
            buckets,
            sum: self.sum.load(Ordering::Relaxed),
            count: cumulative,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_land_in_inclusive_buckets() {
        static BOUNDS: &[u64] = &[10, 20];
        let h = Histogram::new(BOUNDS);
        for v in [0, 10, 11, 20, 21, 1000] {
            h.record(v);
        }
        let s = h.snapshot();
        assert_eq!(s.buckets, vec![(10, 2), (20, 4)]);
        assert_eq!(s.count, 6);
        assert_eq!(s.sum, 1062);
    }

    #[test]
    fn durations_are_microseconds() {
        let h = Histogram::new(RPC_LATENCY_BUCKETS_US);
        h.record_duration(Duration::from_millis(3));
        let s = h.snapshot();
        assert_eq!(s.sum, 3_000);
        assert_eq!(s.buckets[0], (1_000, 0));
        assert_eq!(s.buckets[1], (5_000, 1));
    }
}
