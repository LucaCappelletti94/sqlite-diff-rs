//! Parser for SQLite changeset/patchset binary format.
//!
//! This module provides functionality to parse SQLite session extension
//! changesets and patchsets from their binary representation into
//! [`DiffSetBuilder`] instances.
//!
//! # Binary Format
//!
//! The format consists of one or more table sections:
//!
//! ```text
//! Table Header:
//! ├── Marker: 'T' (0x54) for changeset, 'P' (0x50) for patchset
//! ├── Column count (1 byte)
//! ├── PK flags (1 byte per column: 0x01 = PK, 0x00 = not)
//! └── Table name (null-terminated UTF-8)
//!
//! Change Records (repeated):
//! ├── Operation code: INSERT=0x12, DELETE=0x09, UPDATE=0x17
//! ├── Indirect flag (1 byte, usually 0)
//! └── Values (encoded per operation type)
//! ```
//!

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::hash::Hash;

use crate::builders::DiffSetBuilder;
use crate::builders::{ChangesetFormat, PatchsetFormat, Update};
use crate::encoding::{Value, decode_value, markers, op_codes};
use crate::schema::{DynTable, SchemaWithPK};

/// Errors that can occur during parsing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// Unexpected end of input.
    #[error("Unexpected end of input at position {0}")]
    UnexpectedEof(usize),

    /// Invalid table marker (expected 'T' or 'P').
    #[error("Invalid table marker 0x{0:02x} at position {1}")]
    InvalidTableMarker(u8, usize),

    /// Invalid operation code.
    #[error("Invalid operation code 0x{0:02x} at position {1}")]
    InvalidOpCode(u8, usize),

    /// Invalid UTF-8 in table name.
    #[error("Invalid UTF-8 in table name at position {0}")]
    InvalidTableName(usize),

    /// Failed to decode a value.
    #[error("Failed to decode value at position {0}")]
    InvalidValue(usize),

    /// Table name not null-terminated.
    #[error("Table name not null-terminated")]
    UnterminatedTableName,

    /// Mixed format markers in the same file.
    #[error("Mixed format markers: expected {expected:?}, found {found:?} at position {position}")]
    MixedFormats {
        /// The expected format marker.
        expected: FormatMarker,
        /// The found format marker.
        found: FormatMarker,
        /// The position where the mismatch occurred.
        position: usize,
    },
}

/// The detected format marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatMarker {
    /// Changeset format ('T' marker).
    Changeset,
    /// Patchset format ('P' marker).
    Patchset,
}

/// A table schema parsed from binary changeset/patchset data.
///
/// This type implements [`DynTable`] and [`SchemaWithPK`], allowing it
/// to be used with [`DiffSetBuilder`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedTableSchema {
    /// The table name.
    pub name: String,
    /// Number of columns.
    pub column_count: usize,
    /// Primary key flags - raw bytes from the changeset/patchset.
    ///
    /// Each byte represents the 1-based ordinal position in the composite PK,
    /// or 0 if the column is not part of the primary key.
    /// For example, `[1, 0, 2]` means column 0 is the first PK column,
    /// column 1 is not a PK column, and column 2 is the second PK column.
    pub pk_flags: Vec<u8>,
}

impl ParsedTableSchema {
    /// Create a new parsed table schema.
    #[must_use]
    pub fn new(name: String, column_count: usize, pk_flags: Vec<u8>) -> Self {
        debug_assert_eq!(pk_flags.len(), column_count);
        Self {
            name,
            column_count,
            pk_flags,
        }
    }

    /// Get the indices of primary key columns, in PK order.
    #[must_use]
    pub fn pk_indices(&self) -> Vec<usize> {
        // Collect (col_idx, pk_ordinal) pairs for non-zero entries
        let mut pk_cols: Vec<(usize, u8)> = self
            .pk_flags
            .iter()
            .enumerate()
            .filter_map(|(i, &pk_ordinal)| {
                if pk_ordinal > 0 {
                    Some((i, pk_ordinal))
                } else {
                    None
                }
            })
            .collect();
        // Sort by pk_ordinal to get correct PK order
        pk_cols.sort_by_key(|(_, ordinal)| *ordinal);
        pk_cols.into_iter().map(|(idx, _)| idx).collect()
    }
}

impl DynTable for ParsedTableSchema {
    fn name(&self) -> &str {
        &self.name
    }

    fn number_of_columns(&self) -> usize {
        self.column_count
    }

    fn write_pk_flags(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.column_count);
        buf.copy_from_slice(&self.pk_flags);
    }
}

impl SchemaWithPK for ParsedTableSchema {
    fn extract_pk(&self, values: &[Value]) -> alloc::vec::Vec<Value> {
        self.pk_indices()
            .into_iter()
            .map(|i| values[i].clone())
            .collect()
    }
}

/// A parsed changeset or patchset as a builder.
#[derive(Debug, Clone, Eq)]
pub enum ParsedDiffSet {
    /// A changeset builder.
    Changeset(DiffSetBuilder<ChangesetFormat, ParsedTableSchema>),
    /// A patchset builder.
    Patchset(DiffSetBuilder<PatchsetFormat, ParsedTableSchema>),
}

impl PartialEq for ParsedDiffSet {
    fn eq(&self, other: &Self) -> bool {
        // Empty changesets and patchsets are considered equal since they both
        // serialize to empty bytes and can't be distinguished after roundtrip.
        let self_empty = match self {
            ParsedDiffSet::Changeset(b) => b.is_empty(),
            ParsedDiffSet::Patchset(b) => b.is_empty(),
        };
        let other_empty = match other {
            ParsedDiffSet::Changeset(b) => b.is_empty(),
            ParsedDiffSet::Patchset(b) => b.is_empty(),
        };

        if self_empty && other_empty {
            return true;
        }

        // Otherwise compare by variant and content
        match (self, other) {
            (ParsedDiffSet::Changeset(a), ParsedDiffSet::Changeset(b)) => a == b,
            (ParsedDiffSet::Patchset(a), ParsedDiffSet::Patchset(b)) => a == b,
            _ => false,
        }
    }
}

impl TryFrom<&[u8]> for ParsedDiffSet {
    type Error = ParseError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Self::parse(data)
    }
}

impl From<ParsedDiffSet> for Vec<u8> {
    fn from(diffset: ParsedDiffSet) -> Self {
        match diffset {
            ParsedDiffSet::Changeset(builder) => builder.into(),
            ParsedDiffSet::Patchset(builder) => builder.into(),
        }
    }
}

impl ParsedDiffSet {
    /// Parse binary data into a builder.
    ///
    /// The format (changeset vs patchset) is determined by the first table marker.
    ///
    /// # Errors
    ///
    /// Returns a `ParseError` if the data is malformed or contains invalid values.
    pub fn parse(data: &[u8]) -> Result<Self, ParseError> {
        if data.is_empty() {
            // Empty data defaults to changeset
            return Ok(ParsedDiffSet::Changeset(DiffSetBuilder::new()));
        }

        // Peek at the first byte to determine format
        match data[0] {
            markers::CHANGESET => {
                let builder = parse_as_changeset(data)?;
                Ok(ParsedDiffSet::Changeset(builder))
            }
            markers::PATCHSET => {
                let builder = parse_as_patchset(data)?;
                Ok(ParsedDiffSet::Patchset(builder))
            }
            b => Err(ParseError::InvalidTableMarker(b, 0)),
        }
    }

    /// Returns true if this is a changeset.
    #[must_use]
    pub fn is_changeset(&self) -> bool {
        matches!(self, ParsedDiffSet::Changeset(_))
    }

    /// Returns true if this is a patchset.
    #[must_use]
    pub fn is_patchset(&self) -> bool {
        matches!(self, ParsedDiffSet::Patchset(_))
    }
}

/// Parse binary data as a changeset.
///
/// # Errors
///
/// Returns a `ParseError` if the data is malformed or not a valid changeset.
fn parse_as_changeset(
    data: &[u8],
) -> Result<DiffSetBuilder<ChangesetFormat, ParsedTableSchema>, ParseError> {
    let mut builder = DiffSetBuilder::new();
    let mut pos = 0;

    while pos < data.len() {
        let (schema, format, header_len) = parse_table_header(&data[pos..], pos)?;
        if format != FormatMarker::Changeset {
            return Err(ParseError::MixedFormats {
                expected: FormatMarker::Changeset,
                found: format,
                position: pos,
            });
        }
        pos += header_len;

        while pos < data.len() {
            let byte = data[pos];
            if byte == markers::CHANGESET || byte == markers::PATCHSET {
                break;
            }
            let op_len = parse_changeset_operation(&data[pos..], pos, &schema, &mut builder)?;
            pos += op_len;
        }
    }

    Ok(builder)
}

/// Parse binary data as a patchset.
///
/// # Errors
///
/// Returns a `ParseError` if the data is malformed or not a valid patchset.
fn parse_as_patchset(
    data: &[u8],
) -> Result<DiffSetBuilder<PatchsetFormat, ParsedTableSchema>, ParseError> {
    let mut builder = DiffSetBuilder::new();
    let mut pos = 0;

    while pos < data.len() {
        let (schema, format, header_len) = parse_table_header(&data[pos..], pos)?;
        if format != FormatMarker::Patchset {
            return Err(ParseError::MixedFormats {
                expected: FormatMarker::Patchset,
                found: format,
                position: pos,
            });
        }
        pos += header_len;

        while pos < data.len() {
            let byte = data[pos];
            if byte == markers::CHANGESET || byte == markers::PATCHSET {
                break;
            }
            let op_len = parse_patchset_operation(&data[pos..], pos, &schema, &mut builder)?;
            pos += op_len;
        }
    }

    Ok(builder)
}

/// Parse a table header and return the schema.
fn parse_table_header(
    data: &[u8],
    base_pos: usize,
) -> Result<(ParsedTableSchema, FormatMarker, usize), ParseError> {
    let mut pos = 0;

    if data.is_empty() {
        return Err(ParseError::UnexpectedEof(base_pos));
    }
    let format = match data[pos] {
        markers::CHANGESET => FormatMarker::Changeset,
        markers::PATCHSET => FormatMarker::Patchset,
        b => return Err(ParseError::InvalidTableMarker(b, base_pos + pos)),
    };
    pos += 1;

    if pos >= data.len() {
        return Err(ParseError::UnexpectedEof(base_pos + pos));
    }
    let column_count = data[pos] as usize;
    pos += 1;

    if pos + column_count > data.len() {
        return Err(ParseError::UnexpectedEof(base_pos + pos));
    }
    let pk_flags: Vec<u8> = data[pos..pos + column_count].to_vec();
    pos += column_count;

    let name_start = pos;
    while pos < data.len() && data[pos] != 0 {
        pos += 1;
    }
    if pos >= data.len() {
        return Err(ParseError::UnterminatedTableName);
    }
    let name = String::from_utf8(data[name_start..pos].to_vec())
        .map_err(|_| ParseError::InvalidTableName(base_pos + name_start))?;
    pos += 1;

    Ok((
        ParsedTableSchema::new(name, column_count, pk_flags),
        format,
        pos,
    ))
}

/// Parse operation header (op_code + indirect flag).
fn parse_operation_header(data: &[u8], base_pos: usize) -> Result<(u8, usize), ParseError> {
    if data.len() < 2 {
        return Err(ParseError::UnexpectedEof(base_pos));
    }
    Ok((data[0], 2)) // op_code, skip indirect flag
}

/// Parse a changeset operation.
fn parse_changeset_operation(
    data: &[u8],
    base_pos: usize,
    schema: &ParsedTableSchema,
    builder: &mut DiffSetBuilder<ChangesetFormat, ParsedTableSchema>,
) -> Result<usize, ParseError> {
    let (op_code, mut pos) = parse_operation_header(data, base_pos)?;

    match op_code {
        op_codes::INSERT => {
            let (values, len) = parse_values(&data[pos..], base_pos + pos, schema.column_count)?;
            pos += len;
            *builder = core::mem::take(builder).insert_with_schema(schema.clone(), &values);
        }
        op_codes::DELETE => {
            let (values, len) = parse_values(&data[pos..], base_pos + pos, schema.column_count)?;
            pos += len;
            *builder = core::mem::take(builder).delete_with_schema(schema.clone(), &values);
        }
        op_codes::UPDATE => {
            let (old_values, old_len) =
                parse_values(&data[pos..], base_pos + pos, schema.column_count)?;
            pos += old_len;
            let (new_values, new_len) =
                parse_values(&data[pos..], base_pos + pos, schema.column_count)?;
            pos += new_len;
            *builder = core::mem::take(builder).update_with_schema(
                schema.clone(),
                &old_values,
                &new_values,
            );
        }
        _ => return Err(ParseError::InvalidOpCode(op_code, base_pos)),
    }

    Ok(pos)
}

/// Parse a patchset operation.
fn parse_patchset_operation(
    data: &[u8],
    base_pos: usize,
    schema: &ParsedTableSchema,
    builder: &mut DiffSetBuilder<PatchsetFormat, ParsedTableSchema>,
) -> Result<usize, ParseError> {
    let (op_code, mut pos) = parse_operation_header(data, base_pos)?;

    match op_code {
        op_codes::INSERT => {
            let (values, len) = parse_values(&data[pos..], base_pos + pos, schema.column_count)?;
            pos += len;
            *builder = core::mem::take(builder).insert_with_schema(schema.clone(), &values);
        }
        op_codes::DELETE => {
            // Patchset DELETE: only PK values
            let pk_count = schema.pk_flags.iter().filter(|&&b| b > 0).count();
            let (pk_values, len) = parse_values(&data[pos..], base_pos + pos, pk_count)?;
            pos += len;
            // Expand PK values to full row with Undefined for non-PK columns
            let full_values = expand_pk_values(&schema.pk_flags, pk_values, schema.column_count);
            *builder = core::mem::take(builder).delete_with_schema(schema.clone(), &full_values);
        }
        op_codes::UPDATE => {
            // Note: old_values are parsed but ignored for patchsets since PK is in new_values
            let (_old_values, old_len) =
                parse_values(&data[pos..], base_pos + pos, schema.column_count)?;
            pos += old_len;
            let (new_values, new_len) =
                parse_values(&data[pos..], base_pos + pos, schema.column_count)?;
            pos += new_len;
            // PK is extracted from new_values automatically
            *builder = core::mem::take(builder)
                .update(Update::from_new_values(schema.clone(), new_values));
        }
        _ => return Err(ParseError::InvalidOpCode(op_code, base_pos)),
    }

    Ok(pos)
}

/// Expand PK-only values to full row with Undefined for non-PK columns.
///
/// The pk_flags are raw bytes where non-zero means the column is part of the PK.
/// PK values are expected in the order they appear in pk_flags (not sorted by ordinal).
fn expand_pk_values(pk_flags: &[u8], pk_values: Vec<Value>, column_count: usize) -> Vec<Value> {
    let mut full = vec![Value::Undefined; column_count];
    let mut pk_iter = pk_values.into_iter();
    for (i, &pk_ordinal) in pk_flags.iter().enumerate() {
        if pk_ordinal > 0
            && let Some(v) = pk_iter.next()
        {
            full[i] = v;
        }
    }
    full
}

/// Parse a sequence of values.
fn parse_values(
    data: &[u8],
    base_pos: usize,
    count: usize,
) -> Result<(Vec<Value>, usize), ParseError> {
    let mut values = Vec::with_capacity(count);
    let mut pos = 0;

    for _ in 0..count {
        let (value, value_len) =
            decode_value(&data[pos..]).ok_or(ParseError::InvalidValue(base_pos + pos))?;
        values.push(value);
        pos += value_len;
    }

    Ok((values, pos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_parse_empty() {
        let result = ParsedDiffSet::parse(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_changeset());
    }

    #[test]
    fn test_parse_table_header() {
        // 'T', 2 columns, pk_flags [1, 0], table name "t\0"
        let data = [b'T', 2, 1, 0, b't', 0];
        let (schema, format, len) = parse_table_header(&data, 0).unwrap();

        assert_eq!(format, FormatMarker::Changeset);
        assert_eq!(schema.column_count, 2);
        assert_eq!(schema.pk_flags, vec![1, 0]); // Raw bytes: 1 = first PK column, 0 = not PK
        assert_eq!(schema.name, "t");
        assert_eq!(len, 6);
    }

    #[test]
    fn test_parse_insert_changeset() {
        // Table header + INSERT with integer 1 and text "a"
        let mut data = vec![b'T', 2, 1, 0, b't', 0];
        // INSERT opcode, indirect=0
        data.push(op_codes::INSERT);
        data.push(0);
        // Integer 1 (type 1, 8 bytes)
        data.push(0x01);
        data.extend(&1i64.to_be_bytes());
        // Text "a" (type 3, length 1, "a")
        data.push(0x03);
        data.push(1);
        data.push(b'a');

        let parsed = ParsedDiffSet::parse(&data).unwrap();
        assert!(parsed.is_changeset());
    }

    #[test]
    fn test_parse_delete_changeset() {
        let mut data = vec![b'T', 2, 1, 0, b't', 0];
        data.push(op_codes::DELETE);
        data.push(0);
        // Integer 1
        data.push(0x01);
        data.extend(&1i64.to_be_bytes());
        // Text "a"
        data.push(0x03);
        data.push(1);
        data.push(b'a');

        let parsed = ParsedDiffSet::parse(&data).unwrap();
        assert!(parsed.is_changeset());
    }

    #[test]
    fn test_parse_delete_patchset() {
        // Patchset DELETE only has PK values
        let mut data = vec![b'P', 2, 1, 0, b't', 0];
        data.push(op_codes::DELETE);
        data.push(0);
        // Only PK value (integer 1)
        data.push(0x01);
        data.extend(&1i64.to_be_bytes());

        let parsed = ParsedDiffSet::parse(&data).unwrap();
        assert!(parsed.is_patchset());
    }

    #[test]
    fn test_parse_update_changeset() {
        let mut data = vec![b'T', 2, 1, 0, b't', 0];
        data.push(op_codes::UPDATE);
        data.push(0);
        // Old values: integer 1, text "a"
        data.push(0x01);
        data.extend(&1i64.to_be_bytes());
        data.push(0x03);
        data.push(1);
        data.push(b'a');
        // New values: integer 1, text "b"
        data.push(0x01);
        data.extend(&1i64.to_be_bytes());
        data.push(0x03);
        data.push(1);
        data.push(b'b');

        let parsed = ParsedDiffSet::parse(&data).unwrap();
        assert!(parsed.is_changeset());
    }

    #[test]
    fn test_is_changeset() {
        let data = vec![b'T', 1, 1, b't', 0];
        let parsed = ParsedDiffSet::parse(&data).unwrap();
        assert!(parsed.is_changeset());
        assert!(!parsed.is_patchset());
    }

    #[test]
    fn test_is_patchset() {
        let data = vec![b'P', 1, 1, b't', 0];
        let parsed = ParsedDiffSet::parse(&data).unwrap();
        assert!(parsed.is_patchset());
        assert!(!parsed.is_changeset());
    }

    #[test]
    fn test_parsed_table_schema_dyn_table() {
        let schema = ParsedTableSchema::new("users".into(), 3, vec![1, 0, 0]);
        assert_eq!(schema.name(), "users");
        assert_eq!(schema.number_of_columns(), 3);

        let mut buf = [0u8; 3];
        schema.write_pk_flags(&mut buf);
        assert_eq!(buf, [1, 0, 0]);
    }

    #[test]
    fn test_parsed_table_schema_extract_pk() {
        let schema = ParsedTableSchema::new("users".into(), 3, vec![1, 0, 2]);
        let values = vec![
            Value::Integer(1),
            Value::Text("alice".into()),
            Value::Integer(100),
        ];
        let pk = schema.extract_pk(&values);
        assert_eq!(pk, vec![Value::Integer(1), Value::Integer(100)]);
    }
}
