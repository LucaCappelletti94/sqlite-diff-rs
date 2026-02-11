//! Binary encoding utilities for `SQLite` changeset format.
//!
//! `SQLite` uses specific binary encodings for varints and value serialization.

pub(crate) mod constants;
pub(crate) mod serial;
pub(crate) mod varint;

pub(crate) use constants::{markers, op_codes};
pub use serial::Value;
pub(crate) use serial::{
    MaybeValue, decode_value, encode_defined_value, encode_undefined, encode_value,
};
