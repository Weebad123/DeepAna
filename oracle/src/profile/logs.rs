//! Program logs of one transaction, grouped by invocation.
//!
//! PRISM stores a transaction's log lines as the RPC returned them. The
//! runtime brackets every program invocation:
//!
//! ```text
//! Program <id> invoke [<depth>]
//! Program log: <text>
//! Program data: <base64>
//! Program <id> consumed <n> of <m> compute units
//! Program <id> success            | Program <id> failed: <reason>
//! ```
//!
//! [`group`] turns the flat list into one [`LogGroup`] per `invoke` line, in
//! the order the invocations started (a pre-order walk of the call tree),
//! each holding the lines that belong to it directly, its depth, its outcome
//! and its compute units. Lines outside every invocation are kept apart.
//!
//! Everything here is pure, and nothing here trusts the text: a line that
//! does not parse is still a line of the innermost open invocation, a
//! `success` for a program that is not open closes nothing, and a depth the
//! runtime could not have printed is clamped to the open stack.
//!
//! # Caps
//!
//! A report shows at most [`MAX_LOG_LINES`] lines, each cut to
//! [`MAX_LOG_LINE_CHARS`] characters. The structure (groups, depths,
//! outcomes, compute units, memos) is always built from every line; only
//! the text shown is capped, and [`Logs`] says how much was dropped.
//!
//! # Memos
//!
//! The memo program logs `Memo (len N): "<text>"`, the text in Rust's
//! `Debug` escaping. [`memo_text`] reads it back. The report uses it only
//! when the memo instruction's own data is not available: see
//! [`transaction`](super::transaction).

use serde::Serialize;

/// Most log lines a report shows.
pub const MAX_LOG_LINES: usize = 400;
/// Most characters of one log line a report shows.
pub const MAX_LOG_LINE_CHARS: usize = 1_000;
/// What the runtime writes when it drops lines itself.
pub const RUNTIME_TRUNCATION_MARKER: &str = "Log truncated";

/// How an invocation ended, as far as its lines say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Success,
    Failed,
    /// No closing line: the logs were cut, or an outer failure ended it.
    Unfinished,
}

/// One program invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogGroup {
    /// Position in [`Logs::groups`].
    pub index: usize,
    pub program_id: String,
    /// 1 for a top-level instruction, 2 and up for a cross-program call.
    pub depth: u32,
    pub outcome: Outcome,
    /// The reason of a `failed:` line.
    pub error: Option<String>,
    pub compute_units_consumed: Option<u64>,
    pub compute_units_limit: Option<u64>,
    /// The lines of this invocation itself, its `invoke` and closing lines
    /// included, a nested invocation's excluded. Capped; see the
    /// [module docs](self#caps).
    pub lines: Vec<String>,
}

/// The grouped logs of one transaction.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct Logs {
    /// Lines PRISM stored.
    pub total_lines: usize,
    /// Lines in `groups` and `ungrouped` together.
    pub shown_lines: usize,
    /// Lines cut to [`MAX_LOG_LINE_CHARS`].
    pub shortened_lines: usize,
    /// The runtime itself dropped lines (`Log truncated`).
    pub runtime_truncated: bool,
    /// In the order the invocations started.
    pub groups: Vec<LogGroup>,
    /// Lines outside every invocation.
    pub ungrouped: Vec<String>,
}

impl Logs {
    /// Lines dropped by [`MAX_LOG_LINES`].
    pub const fn dropped_lines(&self) -> usize {
        self.total_lines - self.shown_lines
    }
}

/// What one line is.
#[derive(Debug, PartialEq, Eq)]
enum Line<'a> {
    Invoke { program: &'a str, depth: u32 },
    Success { program: &'a str },
    Failed { program: &'a str, reason: &'a str },
    Consumed { program: &'a str, consumed: u64, limit: u64 },
    Other,
}

fn classify(line: &str) -> Line<'_> {
    let Some(rest) = line.strip_prefix("Program ") else { return Line::Other };
    let Some((program, tail)) = rest.split_once(' ') else { return Line::Other };
    // `Program log:`, `Program data:` and `Program return:` are messages.
    if program.ends_with(':') || program.is_empty() {
        return Line::Other;
    }
    if tail == "success" {
        return Line::Success { program };
    }
    if let Some(reason) = tail.strip_prefix("failed: ") {
        return Line::Failed { program, reason };
    }
    if let Some(depth) = tail.strip_prefix("invoke [").and_then(|d| d.strip_suffix(']')) {
        return match depth.parse::<u32>() {
            Ok(depth) if depth >= 1 => Line::Invoke { program, depth },
            _ => Line::Other,
        };
    }
    if let Some(units) = tail.strip_prefix("consumed ").and_then(|u| u.strip_suffix(" compute units"))
        && let Some((consumed, limit)) = units.split_once(" of ")
        && let (Ok(consumed), Ok(limit)) = (consumed.parse(), limit.parse())
    {
        return Line::Consumed { program, consumed, limit };
    }
    Line::Other
}

/// `line` cut to [`MAX_LOG_LINE_CHARS`] characters, and whether it was cut.
fn shorten(line: &str) -> (String, bool) {
    match line.char_indices().nth(MAX_LOG_LINE_CHARS) {
        None => (line.to_owned(), false),
        Some((at, _)) => {
            let dropped = line[at..].chars().count();
            (format!("{}… [{dropped} more characters]", &line[..at]), true)
        }
    }
}

/// Group `lines` by invocation. See the [module docs](self).
pub fn group(lines: &[String]) -> Logs {
    let mut logs = Logs { total_lines: lines.len(), ..Logs::default() };
    // Indices into `logs.groups` of the invocations still open.
    let mut open: Vec<usize> = Vec::new();

    for line in lines {
        let kind = classify(line);
        if line == RUNTIME_TRUNCATION_MARKER {
            logs.runtime_truncated = true;
        }
        // Which group the line belongs to, and what it changes.
        let owner = match kind {
            Line::Invoke { program, depth } => {
                // The runtime prints depth = open + 1. Anything else means
                // lines are missing: close what the depth says is closed.
                open.truncate((depth as usize).saturating_sub(1));
                let index = logs.groups.len();
                logs.groups.push(LogGroup {
                    index,
                    program_id: program.to_owned(),
                    depth: open.len() as u32 + 1,
                    outcome: Outcome::Unfinished,
                    error: None,
                    compute_units_consumed: None,
                    compute_units_limit: None,
                    lines: Vec::new(),
                });
                open.push(index);
                Some(index)
            }
            Line::Success { program } | Line::Failed { program, .. } => {
                // The innermost open invocation of that program; whatever
                // was opened inside it ended with it.
                match open.iter().rposition(|&g| logs.groups[g].program_id == program) {
                    Some(at) => {
                        let index = open[at];
                        open.truncate(at);
                        let g = &mut logs.groups[index];
                        match kind {
                            Line::Failed { reason, .. } => {
                                g.outcome = Outcome::Failed;
                                g.error = Some(shorten(reason).0);
                            }
                            _ => g.outcome = Outcome::Success,
                        }
                        Some(index)
                    }
                    None => open.last().copied(),
                }
            }
            Line::Consumed { program, consumed, limit } => {
                let index = open.iter().rev().copied().find(|&g| logs.groups[g].program_id == program);
                if let Some(index) = index {
                    let g = &mut logs.groups[index];
                    g.compute_units_consumed = Some(consumed);
                    g.compute_units_limit = Some(limit);
                }
                index.or(open.last().copied())
            }
            Line::Other => open.last().copied(),
        };

        if logs.shown_lines >= MAX_LOG_LINES {
            continue;
        }
        let (text, cut) = shorten(line);
        logs.shown_lines += 1;
        logs.shortened_lines += usize::from(cut);
        match owner {
            Some(index) => logs.groups[index].lines.push(text),
            None => logs.ungrouped.push(text),
        }
    }
    logs
}

/// The memo of a memo program log line, `Program log: Memo (len N): "…"`,
/// unescaped. `None` if the line is not one, or its escaping is malformed.
pub fn memo_text(line: &str) -> Option<String> {
    let rest = line.strip_prefix("Program log: Memo (len ")?;
    let (len, quoted) = rest.split_once("): ")?;
    if len.is_empty() || !len.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    unescape_debug(quoted.strip_prefix('"')?.strip_suffix('"')?)
}

/// Undo Rust's `Debug` escaping of a string's inside.
fn unescape_debug(s: &str) -> Option<String> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next()? {
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            '0' => out.push('\0'),
            '\\' => out.push('\\'),
            '"' => out.push('"'),
            '\'' => out.push('\''),
            'u' => {
                if chars.next()? != '{' {
                    return None;
                }
                let mut hex = String::new();
                loop {
                    match chars.next()? {
                        '}' => break,
                        h if h.is_ascii_hexdigit() && hex.len() < 6 => hex.push(h),
                        _ => return None,
                    }
                }
                out.push(char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?);
            }
            _ => return None,
        }
    }
    Some(out)
}

/// The memos the lines of `group` carry, in order.
pub fn memos_in(lines: &[String]) -> Vec<String> {
    lines.iter().filter_map(|l| memo_text(l)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    const SWAP: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
    const MEMO: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

    fn lines(text: &[&str]) -> Vec<String> {
        text.iter().map(|l| (*l).to_owned()).collect()
    }

    fn shape(logs: &Logs) -> Vec<(&str, u32, Outcome, usize)> {
        logs.groups.iter().map(|g| (g.program_id.as_str(), g.depth, g.outcome, g.lines.len())).collect()
    }

    #[test]
    fn nested_invocations_are_grouped_in_start_order() {
        let logs = group(&lines(&[
            &format!("Program {SWAP} invoke [1]"),
            "Program log: Instruction: Swap",
            &format!("Program {TOKEN} invoke [2]"),
            "Program log: Instruction: Transfer",
            &format!("Program {TOKEN} consumed 4645 of 180000 compute units"),
            &format!("Program {TOKEN} success"),
            "Program data: AQID",
            &format!("Program {TOKEN} invoke [2]"),
            &format!("Program {TOKEN} success"),
            &format!("Program {SWAP} consumed 30000 of 200000 compute units"),
            &format!("Program {SWAP} success"),
            &format!("Program {MEMO} invoke [1]"),
            "Program log: Memo (len 2): \"hi\"",
            &format!("Program {MEMO} success"),
        ]));
        assert_eq!(
            shape(&logs),
            vec![
                (SWAP, 1, Outcome::Success, 5),
                (TOKEN, 2, Outcome::Success, 4),
                (TOKEN, 2, Outcome::Success, 2),
                (MEMO, 1, Outcome::Success, 3),
            ]
        );
        assert_eq!(logs.groups[0].lines[2], "Program data: AQID", "a line after a nested call is the outer one's");
        assert_eq!(
            (logs.groups[0].compute_units_consumed, logs.groups[0].compute_units_limit),
            (Some(30_000), Some(200_000))
        );
        assert_eq!(logs.groups[1].compute_units_consumed, Some(4_645));
        assert_eq!(logs.groups[2].compute_units_consumed, None);
        assert_eq!((logs.total_lines, logs.shown_lines, logs.dropped_lines()), (14, 14, 0));
        assert!(logs.ungrouped.is_empty() && !logs.runtime_truncated);
        assert_eq!(logs.groups.iter().map(|g| g.index).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn a_failure_is_recorded_where_it_happened_and_ends_what_was_open() {
        let logs = group(&lines(&[
            &format!("Program {SWAP} invoke [1]"),
            &format!("Program {TOKEN} invoke [2]"),
            "Program log: Error: insufficient funds",
            &format!("Program {TOKEN} failed: custom program error: 0x1"),
            &format!("Program {SWAP} failed: custom program error: 0x1"),
        ]));
        assert_eq!(shape(&logs), vec![(SWAP, 1, Outcome::Failed, 2), (TOKEN, 2, Outcome::Failed, 3)]);
        assert_eq!(logs.groups[1].error.as_deref(), Some("custom program error: 0x1"));
    }

    #[test]
    fn cut_logs_leave_invocations_unfinished() {
        let logs = group(&lines(&[
            &format!("Program {SWAP} invoke [1]"),
            &format!("Program {TOKEN} invoke [2]"),
            "Log truncated",
        ]));
        assert_eq!(shape(&logs), vec![(SWAP, 1, Outcome::Unfinished, 1), (TOKEN, 2, Outcome::Unfinished, 2)]);
        assert!(logs.runtime_truncated);
    }

    #[test]
    fn text_that_does_not_fit_the_grammar_is_kept_and_changes_nothing() {
        let logs = group(&lines(&[
            "some line before anything",
            &format!("Program {TOKEN} success"), // nothing is open
            &format!("Program {SWAP} invoke [3]"), // depth 3 with nothing open: clamped to 1
            "Program log: Program x invoke [9]",   // a message, not an invocation
            &format!("Program {TOKEN} success"),   // not the open program: closes nothing
            &format!("Program {SWAP} invoke [0]"),
            &format!("Program {SWAP} invoke [two]"),
            &format!("Program {SWAP} consumed lots of units"),
            &format!("Program {SWAP} success"),
            "trailing",
        ]));
        assert_eq!(shape(&logs), vec![(SWAP, 1, Outcome::Success, 7)]);
        assert_eq!(logs.ungrouped, lines(&["some line before anything", &format!("Program {TOKEN} success"), "trailing"]));
        assert_eq!(logs.shown_lines, 10);
    }

    #[test]
    fn a_missing_closing_line_is_repaired_by_the_next_depth() {
        // The token call never closes; the next depth-1 invoke ends both.
        let logs = group(&lines(&[
            &format!("Program {SWAP} invoke [1]"),
            &format!("Program {TOKEN} invoke [2]"),
            &format!("Program {MEMO} invoke [1]"),
            &format!("Program {MEMO} success"),
        ]));
        assert_eq!(
            shape(&logs),
            vec![(SWAP, 1, Outcome::Unfinished, 1), (TOKEN, 2, Outcome::Unfinished, 1), (MEMO, 1, Outcome::Success, 2)]
        );
    }

    #[test]
    fn lines_and_their_length_are_capped_but_the_structure_is_not() {
        let mut text = vec![format!("Program {SWAP} invoke [1]")];
        text.extend((0..MAX_LOG_LINES + 50).map(|i| format!("Program log: {i}")));
        text.push(format!("Program {SWAP} consumed 7 of 9 compute units"));
        text.push(format!("Program {SWAP} success"));
        let logs = group(&text);
        assert_eq!(logs.total_lines, MAX_LOG_LINES + 53);
        assert_eq!(logs.shown_lines, MAX_LOG_LINES);
        assert_eq!(logs.dropped_lines(), 53);
        assert_eq!(logs.groups[0].lines.len(), MAX_LOG_LINES);
        // The closing lines were dropped from the text, not from the structure.
        assert_eq!(logs.groups[0].outcome, Outcome::Success);
        assert_eq!(logs.groups[0].compute_units_consumed, Some(7));

        let long = format!("Program log: {}", "é".repeat(MAX_LOG_LINE_CHARS + 10));
        let logs = group(&[long]);
        assert_eq!(logs.shortened_lines, 1);
        let shown = &logs.ungrouped[0];
        assert!(shown.ends_with("… [23 more characters]"), "{shown}");
        assert_eq!(shown.chars().take_while(|c| *c != '…').count(), MAX_LOG_LINE_CHARS);
    }

    #[test]
    fn memos_are_read_back_from_their_debug_form() {
        assert_eq!(memo_text("Program log: Memo (len 5): \"hello\"").as_deref(), Some("hello"));
        assert_eq!(
            memo_text(r#"Program log: Memo (len 12): "a \"b\"\n\\ \u{1f600}""#).as_deref(),
            Some("a \"b\"\n\\ \u{1f600}")
        );
        assert_eq!(memo_text("Program log: Memo (len 0): \"\"").as_deref(), Some(""));
        for bad in [
            "Program log: Memo (len x): \"a\"",
            "Program log: Memo (len 1): a",
            "Program log: Memo (len 1): \"a",
            r#"Program log: Memo (len 1): "\q""#,
            r#"Program log: Memo (len 1): "\u{110000}""#,
            r#"Program log: Memo (len 1): "\u{12""#,
            "Program log: hello",
            "",
        ] {
            assert_eq!(memo_text(bad), None, "{bad:?}");
        }
        assert_eq!(
            memos_in(&lines(&["Program log: Memo (len 1): \"a\"", "x", "Program log: Memo (len 1): \"b\""])),
            vec!["a", "b"]
        );
    }

    #[test]
    fn grouping_is_a_pure_function_of_the_lines() {
        let text = lines(&[&format!("Program {SWAP} invoke [1]"), "Program log: x", &format!("Program {SWAP} success")]);
        assert_eq!(group(&text), group(&text));
        assert_eq!(group(&[]), Logs::default());
    }
}
