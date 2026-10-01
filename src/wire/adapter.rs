//! [`WireAdapter`]: single-method entry point for decoding one column
//! payload into a [`Value`](crate::encoding::Value).
//!
//! The primary implementer is [`TypeMap`](super::TypeMap). Users who
//! need per-column overrides (rare) implement `WireAdapter` on their
//! own wrapper type.

use super::error::DecodeError;
use super::source::WireSource;
use crate::encoding::Value;

/// Decodes one per-column payload into a [`Value`].
///
/// Object-safe: `dyn WireAdapter<Src, S, B>` works. The primary
/// implementation is [`TypeMap<Src, S, B>`](super::TypeMap), which
/// dispatches on [`WireType`](super::WireType) via a hashmap.
pub trait WireAdapter<Src: WireSource, S, B> {
    /// Decode one column payload.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::NoDecoderForType`] when the underlying
    /// registry has no decoder for the payload's semantic type, or the
    /// specific decoder's own failure mode when it does.
    fn decode(&self, payload: Src::Payload<'_>) -> Result<Value<S, B>, DecodeError>;
}
