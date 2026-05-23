//! Diff inspector. Parses outgoing and incoming changeset byte buffers
//! through sqlite-diff-rs and renders a per-operation summary in a
//! collapsible drawer.
//!
//! The inspector is the most direct showcase of what this crate does:
//! every chat action produces a record here with the SQLite op code,
//! the table name, the primary key, the indirect flag, and the full
//! per-column values that crossed the wire.

use dioxus::prelude::*;
use sqlite_diff_rs::{
    ChangesetOp, ChangesetUpdatePair, ParsedDiffSet, PatchsetOp, TableSchema, Value,
};

/// Direction of an inspected byte buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Locally captured changeset about to be sent.
    Out,
    /// Changeset received from the peer.
    In,
}

impl Direction {
    fn label(self) -> &'static str {
        match self {
            Self::Out => "out",
            Self::In => "in",
        }
    }
}

/// One inspected byte buffer, ready to render.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Page-relative timestamp in milliseconds.
    pub timestamp_ms: u32,
    /// Direction of the buffer.
    pub direction: Direction,
    /// Total byte length of the captured buffer.
    pub byte_count: usize,
    /// Parsed body of the buffer.
    pub body: EntryBody,
    /// For incoming entries that failed to apply locally: the error
    /// message. Outgoing entries and successfully-applied incoming
    /// entries are `None`.
    pub apply_error: Option<String>,
}

/// What the inspector successfully extracted (or failed to extract) from
/// a byte buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryBody {
    /// Empty input, no operations, no parse attempt.
    Empty,
    /// A peer's hello frame carrying their display name.
    Hello {
        /// Name the peer announced.
        name: String,
    },
    /// Parsed changeset or patchset successfully.
    Parsed {
        /// `changeset` or `patchset`.
        format: &'static str,
        /// One [`OpSummary`] per operation in stored order.
        ops: Vec<OpSummary>,
    },
    /// Bytes did not parse as a changeset or patchset.
    ParseError(String),
}

/// Pretty summary of one operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpSummary {
    /// `INSERT`, `UPDATE`, or `DELETE`.
    pub opcode: &'static str,
    /// Table name.
    pub table: String,
    /// Primary-key columns rendered as a `(c1, c2, ...)` string.
    pub pk_repr: String,
    /// SQLite session-extension indirect flag.
    pub indirect: bool,
    /// Per-column lines for the expanded view (column name plus a
    /// short value rendering).
    pub columns: Vec<ColumnLine>,
}

/// One line in the expanded per-column view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnLine {
    /// Column name from the schema, or the index as a fallback.
    pub name: String,
    /// Short rendering of the value(s) at this column. For updates this
    /// is `old -> new`. For undefined columns it is `(unchanged)`.
    pub value: String,
}

/// Parse a byte buffer into an inspector [`Entry`].
#[must_use]
pub fn parse_entry(bytes: &[u8], direction: Direction, timestamp_ms: u32) -> Entry {
    let body = if bytes.is_empty() {
        EntryBody::Empty
    } else {
        match ParsedDiffSet::parse(bytes) {
            Ok(ParsedDiffSet::Changeset(set)) => EntryBody::Parsed {
                format: "changeset",
                ops: set.iter().map(summarize_changeset_op).collect(),
            },
            Ok(ParsedDiffSet::Patchset(set)) => EntryBody::Parsed {
                format: "patchset",
                ops: set.iter().map(summarize_patchset_op).collect(),
            },
            Err(e) => EntryBody::ParseError(format!("{e:?}")),
        }
    };
    Entry {
        timestamp_ms,
        direction,
        byte_count: bytes.len(),
        body,
        apply_error: None,
    }
}

/// Build an inspector entry for a hello frame.
#[must_use]
pub fn hello_entry(
    name: &str,
    byte_count: usize,
    direction: Direction,
    timestamp_ms: u32,
) -> Entry {
    Entry {
        timestamp_ms,
        direction,
        byte_count,
        body: EntryBody::Hello {
            name: name.to_string(),
        },
        apply_error: None,
    }
}

fn summarize_changeset_op(op: ChangesetOp<'_, TableSchema<String>, String, Vec<u8>>) -> OpSummary {
    let table_name = op.table().name().clone();
    let pk_flags = op.table().pk_flags();
    let indirect = op.indirect();

    match op {
        ChangesetOp::Insert { values, .. } => OpSummary {
            opcode: "INSERT",
            table: table_name,
            pk_repr: render_pk(values, pk_flags),
            indirect,
            columns: values
                .iter()
                .enumerate()
                .map(|(idx, v)| ColumnLine {
                    name: column_label(idx),
                    value: render_value(v),
                })
                .collect(),
        },
        ChangesetOp::Update { values, .. } => OpSummary {
            opcode: "UPDATE",
            table: table_name,
            pk_repr: render_pk_from_update(values, pk_flags),
            indirect,
            columns: values
                .iter()
                .enumerate()
                .map(|(idx, (old, new))| ColumnLine {
                    name: column_label(idx),
                    value: render_update_cell(old.as_ref(), new.as_ref()),
                })
                .collect(),
        },
        ChangesetOp::Delete { old_values, .. } => OpSummary {
            opcode: "DELETE",
            table: table_name,
            pk_repr: render_pk(old_values, pk_flags),
            indirect,
            columns: old_values
                .iter()
                .enumerate()
                .map(|(idx, v)| ColumnLine {
                    name: column_label(idx),
                    value: render_value(v),
                })
                .collect(),
        },
    }
}

fn summarize_patchset_op(op: PatchsetOp<'_, TableSchema<String>, String, Vec<u8>>) -> OpSummary {
    let table_name = op.table().name().clone();
    let pk_flags = op.table().pk_flags();
    let indirect = op.indirect();

    match op {
        PatchsetOp::Insert { values, .. } => OpSummary {
            opcode: "INSERT",
            table: table_name,
            pk_repr: render_pk(values, pk_flags),
            indirect,
            columns: values
                .iter()
                .enumerate()
                .map(|(idx, v)| ColumnLine {
                    name: column_label(idx),
                    value: render_value(v),
                })
                .collect(),
        },
        PatchsetOp::Update { pk, entries, .. } => OpSummary {
            opcode: "UPDATE",
            table: table_name,
            pk_repr: pk.iter().map(render_value).collect::<Vec<_>>().join(", "),
            indirect,
            columns: entries
                .iter()
                .enumerate()
                .map(|(idx, ((), new))| ColumnLine {
                    name: column_label(idx),
                    value: match new {
                        Some(v) => render_value(v),
                        None => "(unchanged)".into(),
                    },
                })
                .collect(),
        },
        PatchsetOp::Delete { pk, .. } => OpSummary {
            opcode: "DELETE",
            table: table_name,
            pk_repr: pk.iter().map(render_value).collect::<Vec<_>>().join(", "),
            indirect,
            columns: Vec::new(),
        },
    }
}

fn render_pk(values: &[Value<String, Vec<u8>>], pk_flags: &[u8]) -> String {
    pk_flags
        .iter()
        .enumerate()
        .filter(|(_, flag)| **flag != 0)
        .filter_map(|(idx, _)| values.get(idx))
        .map(render_value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_pk_from_update(
    values: &[ChangesetUpdatePair<String, Vec<u8>>],
    pk_flags: &[u8],
) -> String {
    pk_flags
        .iter()
        .enumerate()
        .filter(|(_, flag)| **flag != 0)
        .filter_map(|(idx, _)| values.get(idx))
        .filter_map(|(old, new)| old.as_ref().or(new.as_ref()))
        .map(render_value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn column_label(idx: usize) -> String {
    format!("col[{idx}]")
}

fn render_value(v: &Value<String, Vec<u8>>) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{f}"),
        Value::Text(s) => format!("\"{s}\""),
        Value::Blob(b) => format!("blob({} bytes)", b.len()),
    }
}

fn render_update_cell(
    old: Option<&Value<String, Vec<u8>>>,
    new: Option<&Value<String, Vec<u8>>>,
) -> String {
    match (old, new) {
        (None, None) => "(unchanged)".into(),
        (Some(a), None) => format!("{} -> (undefined)", render_value(a)),
        (None, Some(b)) => format!("(undefined) -> {}", render_value(b)),
        (Some(a), Some(b)) => format!("{} -> {}", render_value(a), render_value(b)),
    }
}

/// Render an [`Entry`] list as a collapsible drawer.
#[component]
pub fn InspectorPane(entries: Signal<Vec<Entry>>) -> Element {
    rsx! {
        details { style: "margin-top: 1.5rem;",
            summary { style: "cursor: pointer; font-weight: bold;",
                "Diff inspector ({entries.read().len()} entries)"
            }
            ul { style: "list-style: none; padding: 0; font-family: monospace; font-size: 0.8rem;",
                for entry in entries.read().iter().rev().cloned() {
                    EntryRow { entry }
                }
            }
        }
    }
}

#[component]
fn EntryRow(entry: Entry) -> Element {
    let dir_color = match entry.direction {
        Direction::Out => "#2a6f2a",
        Direction::In => "#1f5fa8",
    };
    let row_bg = if entry.apply_error.is_some() {
        "#fff0f0"
    } else {
        "#fafafa"
    };
    rsx! {
        li { style: "margin: 0.25rem 0; padding: 0.25rem; background: {row_bg}; border-left: 3px solid {dir_color};",
            details {
                summary { style: "cursor: pointer;",
                    span { style: "color: {dir_color}; font-weight: bold;", "{entry.direction.label()}" }
                    " · "
                    span { "+{entry.timestamp_ms}ms" }
                    " · "
                    span { "{entry.byte_count} bytes" }
                    " · "
                    EntryHeadline { body: entry.body.clone() }
                    if let Some(err) = entry.apply_error.clone() {
                        span { style: "color: #b00; margin-left: 0.5rem; font-weight: bold;",
                            "apply failed: {err}"
                        }
                    }
                }
                EntryDetail { body: entry.body }
            }
        }
    }
}

#[component]
fn EntryHeadline(body: EntryBody) -> Element {
    match body {
        EntryBody::Empty => rsx! { span { "(empty)" } },
        EntryBody::Hello { name } => rsx! {
            span { style: "color: #2a6f2a; font-weight: bold;", "HELLO" }
            " "
            span { "{name}" }
        },
        EntryBody::ParseError(e) => rsx! { span { style: "color: #b00;", "parse error: {e}" } },
        EntryBody::Parsed { format, ops } => {
            let op_count = ops.len();
            let kinds: Vec<&'static str> = ops.iter().map(|o| o.opcode).collect();
            rsx! { span { "{format} · {op_count} op(s)" if !kinds.is_empty() { " · " } "{kinds.join(\", \")}" } }
        }
    }
}

#[component]
fn EntryDetail(body: EntryBody) -> Element {
    match body {
        EntryBody::Empty | EntryBody::Hello { .. } | EntryBody::ParseError(_) => rsx! {},
        EntryBody::Parsed { ops, .. } => rsx! {
            ul { style: "list-style: none; padding-left: 1rem;",
                for op in ops {
                    OpRow { op }
                }
            }
        },
    }
}

#[component]
fn OpRow(op: OpSummary) -> Element {
    let opcode_color = match op.opcode {
        "INSERT" => "#2a6f2a",
        "UPDATE" => "#a06000",
        "DELETE" => "#a01010",
        _ => "#333",
    };
    rsx! {
        li { style: "margin: 0.25rem 0;",
            div {
                span { style: "color: {opcode_color}; font-weight: bold;", "{op.opcode}" }
                " "
                span { "{op.table}" }
                " "
                span { style: "color: #555;", "pk=({op.pk_repr})" }
                if op.indirect {
                    span { style: "color: #a00; margin-left: 0.5rem;", "[indirect]" }
                }
            }
            if !op.columns.is_empty() {
                ul { style: "list-style: none; padding-left: 1rem; color: #444;",
                    for col in op.columns {
                        li { "{col.name} = {col.value}" }
                    }
                }
            }
        }
    }
}
