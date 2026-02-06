//! Binary encoding utilities for SQLite changeset format.
//!
//! SQLite uses specific binary encodings for varints and value serialization.

pub mod constants;
pub mod serial;
pub mod varint;

pub use constants::{markers, op_codes};
pub use serial::{ChangesetType, SerialType, Value, decode_value, encode_value};
pub use varint::{decode_varint, encode_varint, encode_varint_simple, varint_len};
