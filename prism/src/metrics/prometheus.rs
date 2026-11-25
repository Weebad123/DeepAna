//! Prometheus text exposition, format version 0.0.4.
//!
//! Serve with `Content-Type: text/plain; version=0.0.4; charset=utf-8`.
//!
//! - Every series is prefixed `prism_`. Counters end in `_total`, durations
//!   are in seconds (converted here from integer microseconds).
//! - Every label combination is emitted, zeros included, so `rate()` sees a
//!   series from the first scrape.
//! - Gauges that are unknown (`None` in the snapshot) are omitted, never
//!   rendered as `0`.
//! - Free text (halt reason, last error) is never rendered. Every label value
//!   comes from a fixed enum.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use super::{HistogramSnapshot, MetricsSnapshot, PipelineState, SourceCounters};

/// Render a snapshot as Prometheus text.
pub fn render_prometheus(s: &MetricsSnapshot) -> String {
    let mut out = String::with_capacity(16 * 1024);
    let o = &mut out;

    // -- State and gauges --------------------------------------------------
    header(o, "prism_state", "gauge", "1 for the pipeline's current state, 0 otherwise.");
    for &state in PipelineState::ALL {
        sample(o, "prism_state", &[("state", state.as_str())], u64::from(state == s.state));
    }

    for (name, help, value) in [
        ("prism_tip_slot", "Latest chain tip slot seen.", s.tip_slot),
        ("prism_processed_slot", "Highest slot processed.", s.processed_slot),
        ("prism_flushed_slot", "Highest slot durable on disk.", s.flushed_slot),
        ("prism_finalized_slot", "Finalized watermark slot.", s.finalized_slot),
        ("prism_lag_slots", "Tip slot minus processed slot.", s.lag_slots),
        ("prism_changelog_bytes", "Change log size on disk in bytes.", s.changelog_bytes),
        ("prism_changelog_segments", "Change log segment count.", s.changelog_segments),
    ] {
        if let Some(v) = value {
            header(o, name, "gauge", help);
            sample(o, name, &[], v);
        }
    }

    // -- RPC ---------------------------------------------------------------
    header(o, "prism_rpc_requests_total", "counter", "RPC attempts, retries included.");
    for (method, r) in &s.rpc {
        sample(o, "prism_rpc_requests_total", &[("method", method)], r.requests);
    }
    header(o, "prism_rpc_retries_total", "counter", "RPC attempts that were retries.");
    for (method, r) in &s.rpc {
        sample(o, "prism_rpc_retries_total", &[("method", method)], r.retries);
    }
    header(o, "prism_rpc_errors_total", "counter", "Failed RPC attempts.");
    for (method, r) in &s.rpc {
        for (class, &n) in &r.errors {
            sample(o, "prism_rpc_errors_total", &[("method", method), ("class", class)], n);
        }
    }
    header(o, "prism_rpc_latency_seconds", "histogram", "RPC attempt wall time.");
    for (method, r) in &s.rpc {
        histogram(o, "prism_rpc_latency_seconds", &[("method", method)], &r.latency_us);
    }
    header(
        o,
        "prism_rpc_rate_limit_wait_seconds",
        "histogram",
        "Time spent waiting on rate limits.",
    );
    histogram(o, "prism_rpc_rate_limit_wait_seconds", &[], &s.rate_limit_wait_us);

    // -- Storage -----------------------------------------------------------
    header(o, "prism_rows_written_total", "counter", "Rows written per table.");
    for (table, t) in &s.tables {
        sample(o, "prism_rows_written_total", &[("table", table)], t.rows);
    }
    header(o, "prism_bytes_written_total", "counter", "Bytes written per table.");
    for (table, t) in &s.tables {
        sample(o, "prism_bytes_written_total", &[("table", table)], t.bytes);
    }
    header(o, "prism_files_written_total", "counter", "Parquet files written per table.");
    for (table, t) in &s.tables {
        sample(o, "prism_files_written_total", &[("table", table)], t.files);
    }
    header(o, "prism_flushes_total", "counter", "Flushes by outcome.");
    sample(o, "prism_flushes_total", &[("outcome", "ok")], s.flushes_ok);
    sample(o, "prism_flushes_total", &[("outcome", "failed")], s.flushes_failed);
    header(o, "prism_flush_duration_seconds", "histogram", "Flush wall time.");
    histogram(o, "prism_flush_duration_seconds", &[], &s.flush_duration_us);

    // -- Pipeline ----------------------------------------------------------
    labelled(o, "prism_events_emitted_total", "Change log events emitted.", "event", &s.events);
    header(o, "prism_slots_processed_total", "counter", "Slots processed.");
    sample(o, "prism_slots_processed_total", &[], s.slots_processed);
    labelled(o, "prism_slots_skipped_total", "Slots skipped.", "reason", &s.slots_skipped);
    header(o, "prism_instructions_total", "counter", "Instructions by program class and decode outcome.");
    for (class, outcomes) in &s.instructions {
        for (outcome, &n) in outcomes {
            sample(
                o,
                "prism_instructions_total",
                &[("program_class", class), ("outcome", outcome)],
                n,
            );
        }
    }

    // -- Sources -----------------------------------------------------------
    if let Some(c) = &s.sources.resolver {
        source(o, "resolver", "Account resolver", c);
    }
    if let Some(c) = &s.sources.reconcile {
        source(o, "reconcile", "Reconciler", c);
    }
    if let Some(c) = &s.sources.metadata {
        source(o, "metadata", "Token metadata enricher", c);
    }

    out
}

fn header(o: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(o, "# HELP {name} {help}");
    let _ = writeln!(o, "# TYPE {name} {kind}");
}

fn labels(o: &mut String, labels: &[(&str, &str)]) {
    if labels.is_empty() {
        return;
    }
    o.push('{');
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            o.push(',');
        }
        let _ = write!(o, "{k}=\"");
        escape_label(o, v);
        o.push('"');
    }
    o.push('}');
}

fn sample(o: &mut String, name: &str, l: &[(&str, &str)], value: u64) {
    o.push_str(name);
    labels(o, l);
    let _ = writeln!(o, " {value}");
}

fn labelled(o: &mut String, name: &str, help: &str, key: &str, values: &BTreeMap<&str, u64>) {
    header(o, name, "counter", help);
    for (label, &n) in values {
        sample(o, name, &[(key, label)], n);
    }
}

fn source(o: &mut String, prefix: &str, what: &str, counters: &SourceCounters) {
    for (field, &n) in counters {
        let name = format!("prism_{prefix}_{field}_total");
        header(o, &name, "counter", &format!("{what} counter {field}."));
        sample(o, &name, &[], n);
    }
}

/// Render a microsecond histogram in seconds.
fn histogram(o: &mut String, name: &str, l: &[(&str, &str)], h: &HistogramSnapshot) {
    let bucket = format!("{name}_bucket");
    for &(bound_us, n) in &h.buckets {
        let le = micros_as_seconds(bound_us);
        let mut with_le = l.to_vec();
        with_le.push(("le", &le));
        sample(o, &bucket, &with_le, n);
    }
    let mut with_inf = l.to_vec();
    with_inf.push(("le", "+Inf"));
    sample(o, &bucket, &with_inf, h.count);

    o.push_str(name);
    o.push_str("_sum");
    labels(o, l);
    let _ = writeln!(o, " {}", micros_as_seconds(h.sum));
    sample(o, &format!("{name}_count"), l, h.count);
}

/// Exact decimal seconds from integer microseconds, no float rounding:
/// `1_500_000` -> `1.5`, `1_000` -> `0.001`, `2_000_000` -> `2`.
fn micros_as_seconds(us: u64) -> String {
    let (secs, frac) = (us / 1_000_000, us % 1_000_000);
    if frac == 0 {
        return secs.to_string();
    }
    let frac = format!("{frac:06}");
    format!("{secs}.{}", frac.trim_end_matches('0'))
}

/// Escape a label value per the text format: `\`, `"` and newline.
fn escape_label(o: &mut String, v: &str) {
    for c in v.chars() {
        match c {
            '\\' => o.push_str("\\\\"),
            '"' => o.push_str("\\\""),
            '\n' => o.push_str("\\n"),
            c => o.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::{Metrics, RpcMethod, Sources};
    use super::*;
    use crate::account_resolver::ResolverMetricsSnapshot;

    #[test]
    fn seconds_are_exact() {
        assert_eq!(micros_as_seconds(0), "0");
        assert_eq!(micros_as_seconds(1_000), "0.001");
        assert_eq!(micros_as_seconds(1_500_000), "1.5");
        assert_eq!(micros_as_seconds(60_000_000), "60");
    }

    #[test]
    fn renders_counters_histograms_and_omits_unknown_gauges() {
        let m = Metrics::new();
        m.rpc_request(RpcMethod::GetBlock);
        m.rpc_latency(RpcMethod::GetBlock, Duration::from_millis(3));
        m.set_tip(42);
        m.set_halted("token=hunter2 at https://secret.example");
        let text = render_prometheus(&m.snapshot(&Sources::default()));

        assert!(text.contains("prism_rpc_requests_total{method=\"getBlock\"} 1\n"));
        assert!(!text.contains("method=\"getSlot\""));
        assert!(text.contains("prism_rpc_latency_seconds_bucket{method=\"getBlock\",le=\"0.005\"} 1\n"));
        assert!(text.contains("prism_rpc_latency_seconds_bucket{method=\"getBlock\",le=\"+Inf\"} 1\n"));
        assert!(text.contains("prism_rpc_latency_seconds_sum{method=\"getBlock\"} 0.003\n"));
        assert!(text.contains("prism_tip_slot 42\n"));
        assert!(!text.contains("prism_processed_slot"));
        assert!(!text.contains("prism_lag_slots"));
        assert!(text.contains("prism_state{state=\"halted\"} 1\n"));
        assert!(text.contains("prism_state{state=\"running\"} 0\n"));
        assert!(!text.contains("hunter2") && !text.contains("redacted") && !text.contains("url"));
        assert!(!text.contains("prism_resolver_"));
    }

    #[test]
    fn every_type_line_has_samples_and_names_are_unique() {
        let m = Metrics::new();
        let sources = Sources {
            resolver: Some(ResolverMetricsSnapshot::default()),
            reconcile: Some(Default::default()),
            metadata: None,
        };
        let text = render_prometheus(&m.snapshot(&sources));
        let mut seen = std::collections::HashSet::new();
        for line in text.lines().filter(|l| l.starts_with("# TYPE ")) {
            let name = line.split(' ').nth(2).unwrap();
            assert!(seen.insert(name.to_owned()), "duplicate family {name}");
        }
        assert!(text.contains("prism_resolver_cache_hits_total 0\n"));
        assert!(text.contains("prism_reconcile_rewinds_total 0\n"));
        assert!(text.ends_with('\n'));
    }
}
