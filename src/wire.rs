//! Schema-aware forward conversion from CDC wire formats into
//! [`Value`](crate::encoding::Value).
//!
//! Every supported wire source (`pg_walstream`, `wal2json`, `maxwell`)
//! carries per-column type metadata alongside the raw value bytes. This
//! module funnels the three sources through one shared decoding contract
//! so users can register a single type-to-decoder mapping and consume
//! multiple wire formats interchangeably.
//!
//! # Shape
//!
//! - [`WireSource`] (sealed): per-source marker with an associated
//!   payload struct and type key. Implemented by `PgWalstream`,
//!   `Wal2Json`, `Maxwell`.
//! - [`Decoder`]: one implementation per (source, semantic) pair.
//!   Zero-sized unit types for stateless decoders (`BoolDecoder`,
//!   `IntDecoder`, ...), state-carrying structs for user config.
//! - [`WireAdapter`]: single-method dispatcher fed a per-column
//!   payload, returns a [`Value`](crate::encoding::Value).
//! - [`TypeMap`]: generic hashmap-backed [`WireAdapter`] implementation
//!   keyed by `Src::TypeKey`. The primary user-facing type.
//! - [`TypeMapDefaults`]: per-source `defaults()` builder for a
//!   [`TypeMap`] pre-populated with the crate's self-evident mappings.
mod adapter;
#[cfg(any(feature = "wal2json", feature = "maxwell", feature = "pg-walstream"))]
mod bytes_helpers;
mod decoder;
mod error;
#[cfg(any(feature = "wal2json", feature = "maxwell", feature = "pg-walstream"))]
mod json_helpers;
mod sealed;
mod source;
mod type_map;
#[cfg(any(feature = "wal2json", feature = "maxwell", feature = "pg-walstream"))]
mod uuid_helpers;

#[cfg(feature = "maxwell")]
mod impls_maxwell;
#[cfg(feature = "pg-walstream")]
mod impls_pg_walstream;
#[cfg(feature = "wal2json")]
mod impls_wal2json;

pub use adapter::WireAdapter;
pub use decoder::Decoder;
pub use decoder::{
    BoolDecoder, DateVerbatimDecoder, DecimalTextDecoder, Int64OverflowToTextDecoder, IntDecoder,
    IntervalVerbatimDecoder, JsonCanonicalDecoder, JsonVerbatimDecoder, MySqlBinaryDecoder,
    NullDecoder, PgByteaBinaryDecoder, PgByteaTextModeDecoder, RealDecoder, TextDecoder,
    TimeVerbatimDecoder, TimestampTzVerbatimDecoder, TimestampVerbatimDecoder, UuidBlob16Decoder,
    UuidText36Decoder,
};
pub use error::DecodeError;
#[cfg(any(feature = "wal2json", feature = "maxwell", feature = "pg-walstream"))]
pub(crate) use sealed::Sealed;
pub use source::{Digestable, WireColumnTypes, WireSchema, WireSource};
pub use type_map::{TypeMap, TypeMapDefaults};
