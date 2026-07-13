//! Sealed trait used to gate [`WireSource`](super::WireSource) implementations
//! to types defined in this crate.

/// Marker trait, sealed to the crate.
pub trait Sealed {}
