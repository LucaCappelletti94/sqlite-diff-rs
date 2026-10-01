//! Changeset -> `pgoutput` encoder, inverse of [`pg_walstream`](crate::pg_walstream).
//!
//! Feeds a [`ChangesetOp`] into a [`LogicalReplicationMessage`] ready for
//! `pg_walstream::encode_message`.
//!
//! # Value mapping (protocol v1, text mode)
//!
//! | Wire [`Value`] | Target OID | [`ColumnData`] |
//! |---|---|---|
//! | `Null` | any | `null()` |
//! | `Integer(i)` | [`PG_BOOL`] | `text(b"t")` when non-zero, `text(b"f")` otherwise |
//! | `Integer(i)` | other | `text(i.to_string())` |
//! | `Real(f)` | any | `text(format!("{f}"))` (Rust `Display`) |
//! | `Text(s)` | any | `text(s.as_bytes())` |
//! | `Blob(b)` | [`PG_BYTEA`] | `text("\xHEX")` (lowercase) |
//! | `Blob(b)` | other | `binary(b)` (no validation) |

use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

pub use pg_walstream::{ColumnData, ColumnInfo, LogicalReplicationMessage, Oid, TupleData};

use crate::builders::ChangesetOp;
use crate::encoding::Value;

/// Postgres `BOOL` type OID. Routes [`Value::Integer`] through `t`/`f` text.
pub const PG_BOOL: Oid = 16;

/// Postgres `BYTEA` type OID. Routes [`Value::Blob`] through `\xHEX` text.
pub const PG_BYTEA: Oid = 17;

/// Failure modes of [`op_to_message`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConversionError {
    /// Schema and op disagree on column count.
    #[error("arity mismatch: schema has {schema_arity} columns, op carried {op_arity}")]
    ArityMismatch {
        /// Schema column count.
        schema_arity: usize,
        /// Op column count.
        op_arity: usize,
    },

    /// Fallback slice length differs from schema arity.
    #[error(
        "fallback arity mismatch: schema has {schema_arity} columns, fallback carried {fallback_arity}"
    )]
    FallbackArityMismatch {
        /// Schema column count.
        schema_arity: usize,
        /// Fallback column count.
        fallback_arity: usize,
    },

    /// UPDATE new-side is unchanged and no fallback is available.
    /// `pgoutput` has no legal encoding for an unchanged column on the
    /// new side.
    #[error(
        "unchanged column {column_index} in UPDATE new tuple has no fallback (supply an UpdateFallback)"
    )]
    UnchangedInNewTuple {
        /// Offending column index.
        column_index: usize,
    },
}

/// Per-column Postgres metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnSchema<'a> {
    /// Column name.
    pub name: &'a str,
    /// Postgres type OID.
    pub pg_type_oid: Oid,
    /// Column participates in the primary key. Sets `ColumnInfo::flags` bit 0.
    pub is_pk: bool,
}

/// One Postgres relation, column-order matching the [`ChangesetOp`] arity.
#[derive(Debug, Clone, Copy)]
pub struct RelationSchema<'a> {
    /// Relation OID.
    pub relation_oid: Oid,
    /// Namespace (schema) owning the relation.
    pub namespace: &'a str,
    /// Relation (table) name.
    pub relation_name: &'a str,
    /// Column metadata in column order.
    pub columns: &'a [ColumnSchema<'a>],
}

/// Fallback row for `UPDATE` `(None, _)` / `(_, None)` cells, indexed by
/// column ordinal, must match `schema.columns.len()` when set.
pub type UpdateFallback<'a> = Option<&'a [Value<String, Vec<u8>>]>;

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

/// Map one [`Value`] to a [`ColumnData`] using the destination OID (see
/// module docs for the full table).
#[must_use]
pub fn wire_value_to_column_data<S, B>(value: &Value<S, B>, target_oid: Oid) -> ColumnData
where
    S: AsRef<str>,
    B: AsRef<[u8]>,
{
    match value {
        Value::Null => ColumnData::null(),
        Value::Integer(i) => {
            if target_oid == PG_BOOL {
                let bytes: Vec<u8> = if *i != 0 {
                    b"t".to_vec()
                } else {
                    b"f".to_vec()
                };
                ColumnData::text(bytes)
            } else {
                ColumnData::text(i.to_string().into_bytes())
            }
        }
        Value::Real(f) => ColumnData::text(format!("{f}").into_bytes()),
        Value::Text(s) => ColumnData::text(s.as_ref().as_bytes().to_vec()),
        Value::Blob(b) => {
            let bytes = b.as_ref();
            if target_oid == PG_BYTEA {
                let mut out = String::with_capacity(2 + bytes.len() * 2);
                out.push_str("\\x");
                for byte in bytes {
                    out.push(HEX_LOWER[(byte >> 4) as usize] as char);
                    out.push(HEX_LOWER[(byte & 0x0f) as usize] as char);
                }
                ColumnData::text(out.into_bytes())
            } else {
                ColumnData::binary(bytes.to_vec())
            }
        }
    }
}

/// Build the [`LogicalReplicationMessage::Relation`] the parser needs
/// before any data frame for `schema.relation_oid` decodes.
///
/// Advertises `REPLICA IDENTITY FULL` (`b'f'`) since every changeset
/// carries old images, and emits `type_modifier = -1` (no modifier).
#[must_use]
pub fn relation_message(schema: &RelationSchema<'_>) -> LogicalReplicationMessage {
    let columns = schema
        .columns
        .iter()
        .map(|col| ColumnInfo {
            flags: u8::from(col.is_pk),
            name: Arc::from(col.name),
            type_id: col.pg_type_oid,
            type_modifier: -1,
        })
        .collect();

    LogicalReplicationMessage::Relation {
        relation_id: schema.relation_oid,
        namespace: Arc::from(schema.namespace),
        relation_name: Arc::from(schema.relation_name),
        replica_identity: b'f',
        columns,
    }
}

/// Encode a single [`ChangesetOp`] as a [`LogicalReplicationMessage`].
///
/// `fallback` is consulted only for `Update` ops, on the column indices
/// whose changeset pair has `None`. On the old side a missing value
/// without fallback lowers to [`ColumnData::unchanged`]. On the new side
/// it is an error.
///
/// # Errors
///
/// - [`ConversionError::ArityMismatch`] on schema/op arity mismatch.
/// - [`ConversionError::FallbackArityMismatch`] on fallback/schema
///   arity mismatch.
/// - [`ConversionError::UnchangedInNewTuple`] on an unchanged
///   new-side column without fallback.
pub fn op_to_message<T, S, B>(
    op: &ChangesetOp<'_, T, S, B>,
    schema: &RelationSchema<'_>,
    fallback: UpdateFallback<'_>,
) -> Result<LogicalReplicationMessage, ConversionError>
where
    S: AsRef<str>,
    B: AsRef<[u8]>,
{
    let schema_arity = schema.columns.len();
    match op {
        ChangesetOp::Insert { values, .. } => {
            check_arity(schema_arity, values.len())?;
            let tuple = values_to_tuple(values, schema);
            Ok(LogicalReplicationMessage::Insert {
                relation_id: schema.relation_oid,
                tuple,
            })
        }
        ChangesetOp::Delete { old_values, .. } => {
            check_arity(schema_arity, old_values.len())?;
            let tuple = values_to_tuple(old_values, schema);
            Ok(LogicalReplicationMessage::Delete {
                relation_id: schema.relation_oid,
                old_tuple: tuple,
                key_type: 'O',
            })
        }
        ChangesetOp::Update { values, .. } => {
            check_arity(schema_arity, values.len())?;
            if let Some(fb) = fallback
                && fb.len() != schema_arity
            {
                return Err(ConversionError::FallbackArityMismatch {
                    schema_arity,
                    fallback_arity: fb.len(),
                });
            }

            let mut old_cols: Vec<ColumnData> = Vec::with_capacity(schema_arity);
            let mut new_cols: Vec<ColumnData> = Vec::with_capacity(schema_arity);

            for (column_index, ((old, new), col)) in
                values.iter().zip(schema.columns.iter()).enumerate()
            {
                let oid = col.pg_type_oid;

                let old_data = match old {
                    Some(v) => wire_value_to_column_data(v, oid),
                    None => match fallback {
                        Some(fb) => wire_value_to_column_data(&fb[column_index], oid),
                        None => ColumnData::unchanged(),
                    },
                };
                old_cols.push(old_data);

                let new_data = match new {
                    Some(v) => wire_value_to_column_data(v, oid),
                    None => match fallback {
                        Some(fb) => wire_value_to_column_data(&fb[column_index], oid),
                        None => return Err(ConversionError::UnchangedInNewTuple { column_index }),
                    },
                };
                new_cols.push(new_data);
            }

            Ok(LogicalReplicationMessage::Update {
                relation_id: schema.relation_oid,
                old_tuple: Some(TupleData::new(old_cols)),
                new_tuple: TupleData::new(new_cols),
                key_type: Some('O'),
            })
        }
    }
}

#[inline]
fn check_arity(schema_arity: usize, op_arity: usize) -> Result<(), ConversionError> {
    if schema_arity == op_arity {
        Ok(())
    } else {
        Err(ConversionError::ArityMismatch {
            schema_arity,
            op_arity,
        })
    }
}

fn values_to_tuple<S, B>(values: &[Value<S, B>], schema: &RelationSchema<'_>) -> TupleData
where
    S: AsRef<str>,
    B: AsRef<[u8]>,
{
    let cols: Vec<ColumnData> = values
        .iter()
        .zip(schema.columns.iter())
        .map(|(value, col)| wire_value_to_column_data(value, col.pg_type_oid))
        .collect();
    TupleData::new(cols)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use bytes::BytesMut;
    use pg_walstream::{LogicalReplicationParser, StreamingReplicationMessage, encode_message};

    // Postgres OIDs used in these tests.
    const OID_INT8: Oid = 20;
    const OID_TEXT: Oid = 25;
    const OID_TIMESTAMP: Oid = 1114;

    const RELATION_OID: Oid = 0xC0FFEE;

    // Concrete `(old, new)` pair used in every UPDATE-shaped fixture. Named
    // here to keep clippy::type_complexity quiet on the array literals.
    type UpdatePair = (
        Option<Value<String, Vec<u8>>>,
        Option<Value<String, Vec<u8>>>,
    );

    fn schema<'a>(columns: &'a [ColumnSchema<'a>]) -> RelationSchema<'a> {
        RelationSchema {
            relation_oid: RELATION_OID,
            namespace: "public",
            relation_name: "orders",
            columns,
        }
    }

    fn int_val(i: i64) -> Value<String, Vec<u8>> {
        Value::Integer(i)
    }

    fn text_val(s: &str) -> Value<String, Vec<u8>> {
        Value::Text(s.to_string())
    }

    fn blob_val(b: &[u8]) -> Value<String, Vec<u8>> {
        Value::Blob(b.to_vec())
    }

    #[test]
    fn insert_maps_to_pgoutput_insert_with_full_tuple() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "status",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
            ColumnSchema {
                name: "payload",
                pg_type_oid: PG_BYTEA,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let values = [int_val(7), text_val("paid"), blob_val(&[0xAB, 0xCD])];
        let op = ChangesetOp::Insert {
            table: &table,
            values: &values,
            indirect: false,
        };

        let msg = op_to_message(&op, &s, None).unwrap();
        let LogicalReplicationMessage::Insert { relation_id, tuple } = msg else {
            panic!("expected Insert, got {msg:?}");
        };
        assert_eq!(relation_id, RELATION_OID);
        assert_eq!(tuple.columns.len(), 3);
        assert_eq!(tuple.columns[0].data_type, b't');
        assert_eq!(tuple.columns[0].as_bytes(), b"7");
        assert_eq!(tuple.columns[1].data_type, b't');
        assert_eq!(tuple.columns[1].as_bytes(), b"paid");
        assert_eq!(tuple.columns[2].data_type, b't');
        assert_eq!(tuple.columns[2].as_bytes(), b"\\xabcd");
    }

    #[test]
    fn insert_bool_column_yields_t_or_f_text() {
        let cols = [ColumnSchema {
            name: "active",
            pg_type_oid: PG_BOOL,
            is_pk: false,
        }];
        let s = schema(&cols);
        let table = ();

        let one = [int_val(1)];
        let msg = op_to_message(
            &ChangesetOp::Insert {
                table: &table,
                values: &one,
                indirect: false,
            },
            &s,
            None,
        )
        .unwrap();
        let LogicalReplicationMessage::Insert { tuple, .. } = msg else {
            unreachable!();
        };
        assert_eq!(tuple.columns[0].as_bytes(), b"t");

        let zero = [int_val(0)];
        let msg = op_to_message(
            &ChangesetOp::Insert {
                table: &table,
                values: &zero,
                indirect: false,
            },
            &s,
            None,
        )
        .unwrap();
        let LogicalReplicationMessage::Insert { tuple, .. } = msg else {
            unreachable!();
        };
        assert_eq!(tuple.columns[0].as_bytes(), b"f");
    }

    #[test]
    fn delete_maps_to_pgoutput_delete_with_full_old_tuple() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "amount",
                pg_type_oid: OID_INT8,
                is_pk: false,
            },
            ColumnSchema {
                name: "status",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let old_values = [int_val(1), int_val(100), text_val("pending")];
        let op = ChangesetOp::Delete {
            table: &table,
            old_values: &old_values,
            indirect: false,
        };

        let msg = op_to_message(&op, &s, None).unwrap();
        let LogicalReplicationMessage::Delete {
            relation_id,
            old_tuple,
            key_type,
        } = msg
        else {
            panic!("expected Delete, got {msg:?}");
        };
        assert_eq!(relation_id, RELATION_OID);
        assert_eq!(key_type, 'O');
        assert_eq!(old_tuple.columns.len(), 3);
        assert_eq!(old_tuple.columns[0].as_bytes(), b"1");
        assert_eq!(old_tuple.columns[1].as_bytes(), b"100");
        assert_eq!(old_tuple.columns[2].as_bytes(), b"pending");
    }

    #[test]
    fn update_all_diffed_columns_maps_to_pgoutput_update_with_both_tuples() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "amount",
                pg_type_oid: OID_INT8,
                is_pk: false,
            },
            ColumnSchema {
                name: "status",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let values: [UpdatePair; 3] = [
            (Some(int_val(1)), Some(int_val(1))),
            (Some(int_val(100)), Some(int_val(200))),
            (Some(text_val("pending")), Some(text_val("shipped"))),
        ];
        let op = ChangesetOp::Update {
            table: &table,
            values: &values,
            indirect: false,
        };

        let msg = op_to_message(&op, &s, None).unwrap();
        let LogicalReplicationMessage::Update {
            relation_id,
            old_tuple,
            new_tuple,
            key_type,
        } = msg
        else {
            panic!("expected Update, got {msg:?}");
        };
        assert_eq!(relation_id, RELATION_OID);
        assert_eq!(key_type, Some('O'));
        let old = old_tuple.expect("old tuple present");
        assert_eq!(old.columns[0].as_bytes(), b"1");
        assert_eq!(old.columns[1].as_bytes(), b"100");
        assert_eq!(old.columns[2].as_bytes(), b"pending");
        assert_eq!(new_tuple.columns[0].as_bytes(), b"1");
        assert_eq!(new_tuple.columns[1].as_bytes(), b"200");
        assert_eq!(new_tuple.columns[2].as_bytes(), b"shipped");
    }

    #[test]
    fn update_pk_unchanged_and_one_non_pk_changed_errors_without_fallback() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "amount",
                pg_type_oid: OID_INT8,
                is_pk: false,
            },
            ColumnSchema {
                name: "status",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let values: [UpdatePair; 3] = [
            (Some(int_val(5)), Some(int_val(5))),
            (None, None),
            (Some(text_val("pending")), Some(text_val("shipped"))),
        ];
        let op = ChangesetOp::Update {
            table: &table,
            values: &values,
            indirect: false,
        };

        let err = op_to_message(&op, &s, None).unwrap_err();
        assert_eq!(
            err,
            ConversionError::UnchangedInNewTuple { column_index: 1 }
        );
    }

    #[test]
    fn update_with_fallback_fills_unchanged_columns_on_both_sides() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "amount",
                pg_type_oid: OID_INT8,
                is_pk: false,
            },
            ColumnSchema {
                name: "status",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let values: [UpdatePair; 3] = [
            (Some(int_val(5)), Some(int_val(5))),
            (None, None),
            (Some(text_val("pending")), Some(text_val("shipped"))),
        ];
        let fallback: [Value<String, Vec<u8>>; 3] = [int_val(5), int_val(100), text_val("pending")];
        let op = ChangesetOp::Update {
            table: &table,
            values: &values,
            indirect: false,
        };

        let msg = op_to_message(&op, &s, Some(&fallback)).unwrap();
        let LogicalReplicationMessage::Update {
            old_tuple,
            new_tuple,
            ..
        } = msg
        else {
            unreachable!();
        };
        let old = old_tuple.unwrap();
        assert_eq!(old.columns[0].as_bytes(), b"5");
        assert_eq!(old.columns[1].as_bytes(), b"100");
        assert_eq!(old.columns[2].as_bytes(), b"pending");
        assert_eq!(new_tuple.columns[0].as_bytes(), b"5");
        assert_eq!(new_tuple.columns[1].as_bytes(), b"100");
        assert_eq!(new_tuple.columns[2].as_bytes(), b"shipped");
    }

    #[test]
    fn update_pk_change_carries_old_pk_in_old_tuple_and_new_pk_in_new_tuple() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "amount",
                pg_type_oid: OID_INT8,
                is_pk: false,
            },
            ColumnSchema {
                name: "status",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let values: [UpdatePair; 3] = [
            (Some(int_val(5)), Some(int_val(6))),
            (None, None),
            (None, None),
        ];
        let fallback: [Value<String, Vec<u8>>; 3] = [int_val(5), int_val(100), text_val("paid")];
        let op = ChangesetOp::Update {
            table: &table,
            values: &values,
            indirect: false,
        };

        let msg = op_to_message(&op, &s, Some(&fallback)).unwrap();
        let LogicalReplicationMessage::Update {
            old_tuple,
            new_tuple,
            ..
        } = msg
        else {
            unreachable!();
        };
        let old = old_tuple.unwrap();
        assert_eq!(old.columns[0].as_bytes(), b"5");
        assert_eq!(old.columns[1].as_bytes(), b"100");
        assert_eq!(old.columns[2].as_bytes(), b"paid");
        assert_eq!(new_tuple.columns[0].as_bytes(), b"6");
        assert_eq!(new_tuple.columns[1].as_bytes(), b"100");
        assert_eq!(new_tuple.columns[2].as_bytes(), b"paid");
    }

    #[test]
    fn arity_mismatch_op_wider_than_schema_errors() {
        let cols = [
            ColumnSchema {
                name: "a",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "b",
                pg_type_oid: OID_INT8,
                is_pk: false,
            },
            ColumnSchema {
                name: "c",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let values = [int_val(1), int_val(2), text_val("x"), int_val(4)];
        let op = ChangesetOp::Insert {
            table: &table,
            values: &values,
            indirect: false,
        };

        let err = op_to_message(&op, &s, None).unwrap_err();
        assert_eq!(
            err,
            ConversionError::ArityMismatch {
                schema_arity: 3,
                op_arity: 4,
            }
        );
    }

    #[test]
    fn fallback_arity_mismatch_errors() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "amount",
                pg_type_oid: OID_INT8,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let values: [UpdatePair; 2] = [(Some(int_val(5)), Some(int_val(5))), (None, None)];
        let fallback: [Value<String, Vec<u8>>; 1] = [int_val(100)];
        let op = ChangesetOp::Update {
            table: &table,
            values: &values,
            indirect: false,
        };

        let err = op_to_message(&op, &s, Some(&fallback)).unwrap_err();
        assert_eq!(
            err,
            ConversionError::FallbackArityMismatch {
                schema_arity: 2,
                fallback_arity: 1,
            }
        );
    }

    #[test]
    fn wire_value_null_maps_to_column_data_null_regardless_of_type_oid() {
        for oid in [OID_INT8, OID_TEXT, PG_BOOL, PG_BYTEA, OID_TIMESTAMP] {
            let cd = wire_value_to_column_data::<String, Vec<u8>>(&Value::Null, oid);
            assert_eq!(cd.data_type, b'n', "oid {oid}");
            assert!(cd.as_bytes().is_empty(), "oid {oid}");
        }
    }

    #[test]
    fn blob_with_non_bytea_oid_falls_through_as_binary_column_data() {
        let cd =
            wire_value_to_column_data::<String, Vec<u8>>(&Value::Blob(vec![1, 2, 3]), OID_TEXT);
        assert_eq!(cd.data_type, b'b');
        assert_eq!(cd.as_bytes(), &[1, 2, 3]);
    }

    #[test]
    fn real_uses_display_formatting() {
        let cd = wire_value_to_column_data::<String, Vec<u8>>(&Value::Real(1.5), OID_INT8);
        assert_eq!(cd.data_type, b't');
        assert_eq!(cd.as_bytes(), b"1.5");
    }

    #[test]
    fn relation_message_builds_replica_identity_full_and_pk_flags() {
        let cols = [
            ColumnSchema {
                name: "a",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "b",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "c",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let msg = relation_message(&s);
        let LogicalReplicationMessage::Relation {
            relation_id,
            namespace,
            relation_name,
            replica_identity,
            columns,
        } = msg
        else {
            panic!("expected Relation, got {msg:?}");
        };
        assert_eq!(relation_id, RELATION_OID);
        assert_eq!(&*namespace, "public");
        assert_eq!(&*relation_name, "orders");
        assert_eq!(replica_identity, b'f');
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].flags, 1);
        assert_eq!(&*columns[0].name, "a");
        assert_eq!(columns[0].type_id, OID_INT8);
        assert_eq!(columns[0].type_modifier, -1);
        assert_eq!(columns[1].flags, 1);
        assert_eq!(columns[2].flags, 0);
    }

    // Encode `msg` through `pg_walstream::encode_message` and parse the
    // bytes back through a fresh `LogicalReplicationParser`. Asserts the
    // parser accepts the frame and returns the same value.
    fn round_trip(msg: &LogicalReplicationMessage) -> LogicalReplicationMessage {
        let mut buf = BytesMut::new();
        encode_message(msg, 1, &mut buf);
        let mut parser = LogicalReplicationParser::with_protocol_version(1);
        let StreamingReplicationMessage { message, .. } =
            parser.parse_wal_message(&buf).expect("parse");
        message
    }

    #[test]
    fn roundtrip_relation_insert_update_delete_parses_back_semantically() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "amount",
                pg_type_oid: OID_INT8,
                is_pk: false,
            },
            ColumnSchema {
                name: "status",
                pg_type_oid: OID_TEXT,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();

        // Relation frame parses back byte-for-byte.
        let rel = relation_message(&s);
        assert_eq!(round_trip(&rel), rel);

        // Insert.
        let ins_values = [int_val(7), int_val(500), text_val("paid")];
        let ins = op_to_message(
            &ChangesetOp::Insert {
                table: &table,
                values: &ins_values,
                indirect: false,
            },
            &s,
            None,
        )
        .unwrap();
        assert_eq!(round_trip(&ins), ins);

        // Update with a fallback covering an unchanged non-PK column.
        let upd_values: [UpdatePair; 3] = [
            (Some(int_val(7)), Some(int_val(7))),
            (None, None),
            (Some(text_val("paid")), Some(text_val("shipped"))),
        ];
        let fallback: [Value<String, Vec<u8>>; 3] = [int_val(7), int_val(500), text_val("paid")];
        let upd = op_to_message(
            &ChangesetOp::Update {
                table: &table,
                values: &upd_values,
                indirect: false,
            },
            &s,
            Some(&fallback),
        )
        .unwrap();
        assert_eq!(round_trip(&upd), upd);

        // Delete.
        let del_values = [int_val(7), int_val(500), text_val("shipped")];
        let del = op_to_message(
            &ChangesetOp::Delete {
                table: &table,
                old_values: &del_values,
                indirect: false,
            },
            &s,
            None,
        )
        .unwrap();
        assert_eq!(round_trip(&del), del);
    }

    #[test]
    fn roundtrip_bytea_column_survives_hex_encoding() {
        let cols = [
            ColumnSchema {
                name: "id",
                pg_type_oid: OID_INT8,
                is_pk: true,
            },
            ColumnSchema {
                name: "payload",
                pg_type_oid: PG_BYTEA,
                is_pk: false,
            },
        ];
        let s = schema(&cols);
        let table = ();
        let values = [int_val(1), blob_val(&[0x00, 0x0f, 0xff, 0xab, 0xcd])];
        let ins = op_to_message(
            &ChangesetOp::Insert {
                table: &table,
                values: &values,
                indirect: false,
            },
            &s,
            None,
        )
        .unwrap();
        let LogicalReplicationMessage::Insert { tuple, .. } = &ins else {
            unreachable!();
        };
        assert_eq!(tuple.columns[1].as_bytes(), b"\\x000fffabcd");
        assert_eq!(round_trip(&ins), ins);
    }
}
