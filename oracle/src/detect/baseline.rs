//! Scoring: the robust baseline every series detector is judged against.
//!
//! Pure functions only: no I/O, no async, no store. A value is compared with
//! the median of its history, scaled by the history's median absolute
//! deviation (MAD), as Iglewicz and Hoaglin's *modified z-score*:
//!
//! ```text
//! z = 0.6745 * (value - median) / scale
//! scale = max(MAD, min_scale, rel_floor * |median|)
//! ```
//!
//! `0.6745` is the 0.75 quantile of the standard normal, so for normally
//! distributed data `z` is on the same footing as an ordinary z-score, and
//! the usual cut-off is 3.5.
//!
//! Median and MAD are used instead of mean and standard deviation because
//! one past spike moves them barely at all: an anomaly cannot hide the next
//! one by inflating the baseline.
//!
//! # The scale floors
//!
//! A perfectly steady history has a MAD of 0, and any deviation at all
//! would score infinitely. Two floors keep the scale meaningful:
//!
//! * `min_scale`, an absolute floor in the metric's own unit (1 transaction,
//!   0.01 of a ratio). It must be positive, so the scale never reaches 0.
//! * `rel_floor * |median|`, a relative floor: a series that is steady at
//!   10 000 should not flag a move to 10 050.
//!
//! # Boundaries
//!
//! A value is anomalous only when `|z|` is **strictly** above the
//! threshold. A history shorter than `min_history` gives no verdict at all,
//! and so does any non-finite input or result: a NaN never becomes a
//! "normal" reading by accident.
//!
//! The scale returned with an anomaly is the effective scale actually
//! divided by, so `0.6745 * (value - median) / scale` recomputed from the
//! stored row is bit-for-bit the stored score.

use serde::Serialize;

/// The constant of the modified z-score: the 0.75 quantile of N(0, 1).
pub const MODIFIED_Z_FACTOR: f64 = 0.6745;

/// Which deviations count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Both a spike and a drop.
    Both,
    /// Only values above the median.
    Up,
    /// Only values below the median.
    Down,
}

impl Direction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::Up => "up",
            Self::Down => "down",
        }
    }

    /// Whether a deviation scoring `z` points the way this direction watches.
    /// `z = 0` points nowhere.
    pub fn admits(self, z: f64) -> bool {
        match self {
            Self::Both => z != 0.0,
            Self::Up => z > 0.0,
            Self::Down => z < 0.0,
        }
    }
}

/// How one series is scored.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ScoreParams {
    /// `|z|` must be strictly above this. Positive and finite.
    pub threshold: f64,
    /// Fewest history values a verdict needs. At least 1.
    pub min_history: usize,
    /// Absolute floor of the scale, in the metric's unit. Positive and finite.
    pub min_scale: f64,
    /// Relative floor of the scale, as a fraction of `|median|`. Finite and
    /// at least 0.
    pub rel_floor: f64,
    pub direction: Direction,
}

impl ScoreParams {
    /// Check every rule above. A detector's params are constants, so this is
    /// for tests and for params built from config.
    pub fn validate(&self) -> Result<(), String> {
        if !(self.threshold.is_finite() && self.threshold > 0.0) {
            return Err(format!("threshold {} must be positive and finite", self.threshold));
        }
        if self.min_history == 0 {
            return Err("min_history must be at least 1".to_owned());
        }
        if !(self.min_scale.is_finite() && self.min_scale > 0.0) {
            return Err(format!("min_scale {} must be positive and finite", self.min_scale));
        }
        if !(self.rel_floor.is_finite() && self.rel_floor >= 0.0) {
            return Err(format!("rel_floor {} must be finite and at least 0", self.rel_floor));
        }
        Ok(())
    }
}

/// Why a value got no verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum NoVerdict {
    /// Fewer history values than `min_history`.
    InsufficientHistory { have: usize, need: usize },
    /// The value, or a history value, is NaN or infinite.
    NonFiniteValue,
    /// The inputs were finite but the score was not (an overflow, or a
    /// scale of 0 from params that skipped validation).
    NonFiniteResult,
}

/// The outcome of [`score`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    NoVerdict(NoVerdict),
    /// Within the threshold, or outside it the other way from `direction`.
    Normal { z: f64 },
    Anomalous {
        z: f64,
        median: f64,
        /// The raw MAD of the history.
        mad: f64,
        /// The effective scale `z` was divided by (see the module docs).
        scale: f64,
        /// [`Direction::Up`] or [`Direction::Down`]: which way it moved.
        direction: Direction,
    },
}

impl Verdict {
    pub const fn is_anomalous(&self) -> bool {
        matches!(self, Self::Anomalous { .. })
    }

    /// The score, when there is one.
    pub const fn z(&self) -> Option<f64> {
        match *self {
            Self::NoVerdict(_) => None,
            Self::Normal { z } | Self::Anomalous { z, .. } => Some(z),
        }
    }
}

/// The median of `values`; `None` when empty or when any value is NaN.
/// An even count averages the middle two.
pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() || values.iter().any(|v| v.is_nan()) {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        midpoint(sorted[mid - 1], sorted[mid])
    })
}

/// The median absolute deviation of `values` around `median`. `0.0` for an
/// empty slice; NaN if any value (or `median`) is NaN.
pub fn mad(values: &[f64], median: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let deviations: Vec<f64> = values.iter().map(|v| (v - median).abs()).collect();
    self::median(&deviations).unwrap_or(f64::NAN)
}

/// The scale [`score`] divides by: `max(mad, min_scale, rel_floor * |median|)`.
pub fn effective_scale(mad: f64, median: f64, params: &ScoreParams) -> f64 {
    // `f64::max` would drop a NaN; keep it, so it surfaces as NonFiniteResult.
    if mad.is_nan() || median.is_nan() {
        return f64::NAN;
    }
    mad.max(params.min_scale).max(params.rel_floor * median.abs())
}

/// Score `value` against `history` (order does not matter).
pub fn score(history: &[f64], value: f64, params: &ScoreParams) -> Verdict {
    if !value.is_finite() || history.iter().any(|v| !v.is_finite()) {
        return Verdict::NoVerdict(NoVerdict::NonFiniteValue);
    }
    if history.len() < params.min_history || history.is_empty() {
        return Verdict::NoVerdict(NoVerdict::InsufficientHistory {
            have: history.len(),
            need: params.min_history.max(1),
        });
    }
    let Some(median) = median(history) else {
        return Verdict::NoVerdict(NoVerdict::NonFiniteValue);
    };
    let mad = mad(history, median);
    let scale = effective_scale(mad, median, params);
    let z = modified_z(value, median, scale);
    if !z.is_finite() || !scale.is_finite() || scale <= 0.0 {
        return Verdict::NoVerdict(NoVerdict::NonFiniteResult);
    }
    if z.abs() > params.threshold && params.direction.admits(z) {
        Verdict::Anomalous {
            z,
            median,
            mad,
            scale,
            direction: if z > 0.0 { Direction::Up } else { Direction::Down },
        }
    } else {
        Verdict::Normal { z }
    }
}

/// `0.6745 * (value - median) / scale`, the one formula both [`score`] and
/// anyone checking a stored anomaly use.
pub fn modified_z(value: f64, median: f64, scale: f64) -> f64 {
    MODIFIED_Z_FACTOR * (value - median) / scale
}

/// `(a + b) / 2` without overflowing for huge same-sign values.
fn midpoint(a: f64, b: f64) -> f64 {
    let sum = a + b;
    if sum.is_finite() { sum / 2.0 } else { a / 2.0 + b / 2.0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(threshold: f64, direction: Direction) -> ScoreParams {
        ScoreParams { threshold, min_history: 5, min_scale: 1e-9, rel_floor: 0.0, direction }
    }

    // -- median / mad --------------------------------------------------------

    #[test]
    fn median_of_odd_even_and_degenerate_inputs() {
        assert_eq!(median(&[]), None);
        assert_eq!(median(&[7.0]), Some(7.0));
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), Some(2.5));
        assert_eq!(median(&[1.0, f64::NAN]), None);
        // Order does not matter, duplicates count.
        assert_eq!(median(&[5.0, 5.0, 1.0, 9.0, 5.0]), Some(5.0));
        // The midpoint of huge values does not overflow.
        assert_eq!(median(&[f64::MAX, f64::MAX]), Some(f64::MAX));
        assert_eq!(median(&[-0.0, 0.0]), Some(0.0));
    }

    #[test]
    fn mad_matches_hand_computation() {
        // |x - 3| = 2,1,0,1,6 -> sorted 0,1,1,2,6 -> 1
        let xs = [1.0, 2.0, 3.0, 4.0, 9.0];
        assert_eq!(median(&xs), Some(3.0));
        assert_eq!(mad(&xs, 3.0), 1.0);
        assert_eq!(mad(&[], 3.0), 0.0);
        assert_eq!(mad(&[4.0, 4.0, 4.0], 4.0), 0.0);
        assert!(mad(&[1.0], f64::NAN).is_nan());
    }

    #[test]
    fn effective_scale_takes_the_largest_floor() {
        let mut p = params(3.5, Direction::Both);
        p.min_scale = 1.0;
        p.rel_floor = 0.05;
        assert_eq!(effective_scale(4.0, 10.0, &p), 4.0);
        assert_eq!(effective_scale(0.0, 10.0, &p), 1.0);
        assert_eq!(effective_scale(0.0, 1_000.0, &p), 50.0);
        assert_eq!(effective_scale(0.0, -1_000.0, &p), 50.0);
        assert!(effective_scale(f64::NAN, 1.0, &p).is_nan());
    }

    // -- numeric accuracy ----------------------------------------------------

    #[test]
    fn score_is_numerically_exact_against_a_hand_worked_example() {
        // history 10,12,11,13,9,10,14 -> sorted 9,10,10,11,12,13,14: median 11
        // |x-11| = 1,1,0,2,2,1,3 -> sorted 0,1,1,1,2,2,3: MAD 1
        let history = [10.0, 12.0, 11.0, 13.0, 9.0, 10.0, 14.0];
        let verdict = score(&history, 20.0, &params(3.5, Direction::Both));
        let Verdict::Anomalous { z, median, mad, scale, direction } = verdict else {
            panic!("{verdict:?}");
        };
        assert_eq!((median, mad, scale, direction), (11.0, 1.0, 1.0, Direction::Up));
        // 0.6745 * 9 / 1, computed the same way, compared exactly.
        assert_eq!(z, 0.6745 * 9.0);
        assert!((z - 6.0705).abs() < 1e-12, "{z}");
    }

    #[test]
    fn score_agrees_with_a_naive_reference_on_many_series() {
        // A deterministic LCG, so the test needs no RNG crate and never flakes.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            (state >> 11) as f64 / (1u64 << 53) as f64
        };
        for case in 0..500 {
            let len = 5 + case % 40;
            let base = 1.0 + next() * 1_000.0;
            let history: Vec<f64> = (0..len).map(|_| base + (next() - 0.5) * base * 0.2).collect();
            let value = base + (next() - 0.5) * base;

            // Reference: sort, take middle by the textbook definition.
            let reference_median = |xs: &[f64]| {
                let mut s = xs.to_vec();
                s.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let n = s.len();
                if n % 2 == 1 { s[n / 2] } else { (s[n / 2 - 1] + s[n / 2]) / 2.0 }
            };
            let m = reference_median(&history);
            let devs: Vec<f64> = history.iter().map(|x| (x - m).abs()).collect();
            let d = reference_median(&devs);
            let p = ScoreParams { threshold: 3.5, min_history: 5, min_scale: 1e-6, rel_floor: 0.0, direction: Direction::Both };
            let expected = 0.6745 * (value - m) / d.max(1e-6);

            let got = score(&history, value, &p).z().unwrap();
            let tolerance = 1e-12 * expected.abs().max(1.0);
            assert!((got - expected).abs() <= tolerance, "case {case}: {got} vs {expected}");
        }
    }

    #[test]
    fn an_anomaly_reproduces_its_score_from_value_median_and_scale() {
        let history = [100.0, 104.0, 98.0, 101.0, 97.0, 103.0, 99.0, 102.0];
        let p = ScoreParams { threshold: 3.5, min_history: 5, min_scale: 1.0, rel_floor: 0.05, direction: Direction::Both };
        for value in [0.0, 50.0, 140.0, 1e6] {
            if let Verdict::Anomalous { z, median, scale, .. } = score(&history, value, &p) {
                assert_eq!(modified_z(value, median, scale).to_bits(), z.to_bits(), "{value}");
            }
        }
    }

    #[test]
    fn one_past_spike_barely_moves_the_baseline() {
        let calm = [10.0, 11.0, 9.0, 10.0, 12.0, 10.0, 11.0, 9.0, 10.0];
        let mut spiked = calm.to_vec();
        spiked[4] = 1e9;
        let p = params(3.5, Direction::Both);
        assert!(score(&calm, 30.0, &p).is_anomalous());
        assert!(score(&spiked, 30.0, &p).is_anomalous(), "a past spike must not mask the next one");
    }

    // -- boundaries ----------------------------------------------------------

    /// A history whose median is 0 and whose MAD is exactly 1, so `z` is
    /// exactly `0.6745 * value`.
    const UNIT: [f64; 5] = [-1.0, -1.0, 0.0, 1.0, 1.0];

    #[test]
    fn the_threshold_is_strict() {
        // Choose a value whose z equals a threshold bit-for-bit.
        let value = 4.0;
        let at = modified_z(value, 0.0, 1.0);
        let p = params(at, Direction::Both);
        assert_eq!(score(&UNIT, value, &p), Verdict::Normal { z: at }, "exactly at the threshold is normal");
        let above = params(f64::from_bits(at.to_bits() - 1), Direction::Both);
        assert!(score(&UNIT, value, &above).is_anomalous(), "one ulp below the score is anomalous");
        assert!(score(&UNIT, -value, &above).is_anomalous());
        assert!(!score(&UNIT, -value, &p).is_anomalous());
    }

    #[test]
    fn direction_filters_by_sign_and_reports_the_actual_one() {
        let up = params(3.5, Direction::Up);
        let down = params(3.5, Direction::Down);
        let both = params(3.5, Direction::Both);
        assert!(matches!(score(&UNIT, 10.0, &up), Verdict::Anomalous { direction: Direction::Up, .. }));
        assert!(matches!(score(&UNIT, -10.0, &up), Verdict::Normal { .. }));
        assert!(matches!(score(&UNIT, -10.0, &down), Verdict::Anomalous { direction: Direction::Down, .. }));
        assert!(matches!(score(&UNIT, 10.0, &down), Verdict::Normal { .. }));
        assert!(matches!(score(&UNIT, -10.0, &both), Verdict::Anomalous { direction: Direction::Down, .. }));
        assert_eq!(score(&UNIT, 0.0, &both), Verdict::Normal { z: 0.0 });
    }

    #[test]
    fn min_history_is_inclusive() {
        let p = params(3.5, Direction::Both);
        assert_eq!(
            score(&UNIT[..4], 100.0, &p),
            Verdict::NoVerdict(NoVerdict::InsufficientHistory { have: 4, need: 5 })
        );
        assert!(score(&UNIT, 100.0, &p).is_anomalous(), "exactly min_history values are enough");
        assert_eq!(
            score(&[], 1.0, &ScoreParams { min_history: 0, ..p }),
            Verdict::NoVerdict(NoVerdict::InsufficientHistory { have: 0, need: 1 })
        );
    }

    #[test]
    fn non_finite_inputs_and_results_give_no_verdict() {
        let p = params(3.5, Direction::Both);
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(score(&UNIT, bad, &p), Verdict::NoVerdict(NoVerdict::NonFiniteValue));
            let mut history = UNIT.to_vec();
            history.push(bad);
            assert_eq!(score(&history, 1.0, &p), Verdict::NoVerdict(NoVerdict::NonFiniteValue));
        }
        // Finite inputs whose difference overflows.
        let huge = [f64::MAX; 5];
        assert_eq!(score(&huge, -f64::MAX, &p), Verdict::NoVerdict(NoVerdict::NonFiniteResult));
        // A zero scale (params that skipped validation) is refused, not divided by.
        let flat = [5.0; 5];
        let zero = ScoreParams { min_scale: 0.0, ..p };
        assert_eq!(score(&flat, 5.0, &zero), Verdict::NoVerdict(NoVerdict::NonFiniteResult));
        assert_eq!(score(&flat, 6.0, &zero), Verdict::NoVerdict(NoVerdict::NonFiniteResult));
    }

    #[test]
    fn a_flat_history_uses_the_floors_instead_of_dividing_by_zero() {
        let flat = [100.0; 10];
        let p = ScoreParams { threshold: 3.5, min_history: 5, min_scale: 1.0, rel_floor: 0.05, direction: Direction::Both };
        // scale = max(0, 1, 5) = 5
        assert_eq!(score(&flat, 110.0, &p), Verdict::Normal { z: 0.6745 * 10.0 / 5.0 });
        let Verdict::Anomalous { scale, mad, .. } = score(&flat, 130.0, &p) else { panic!() };
        assert_eq!((scale, mad), (5.0, 0.0));
    }

    #[test]
    fn params_validation_names_the_bad_field() {
        let ok = params(3.5, Direction::Both);
        assert!(ok.validate().is_ok());
        assert!(ScoreParams { threshold: 0.0, ..ok }.validate().unwrap_err().contains("threshold"));
        assert!(ScoreParams { threshold: f64::NAN, ..ok }.validate().is_err());
        assert!(ScoreParams { min_history: 0, ..ok }.validate().unwrap_err().contains("min_history"));
        assert!(ScoreParams { min_scale: 0.0, ..ok }.validate().unwrap_err().contains("min_scale"));
        assert!(ScoreParams { rel_floor: -0.1, ..ok }.validate().unwrap_err().contains("rel_floor"));
    }
}
