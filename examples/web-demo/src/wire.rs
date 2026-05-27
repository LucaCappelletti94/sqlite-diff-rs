// Items in this module are wired up by the App-scope code; some helper
// methods are exercised only by tests.
#![allow(dead_code)]

//! Wire envelope for inter-peer messages on the WebRTC data channel.
//!
//! Every byte buffer crossing the data channel is framed as:
//!
//! ```text
//! [version: u8 = 1]
//! [msg_id: 16 bytes (UUIDv4)]
//! [kind: u8 = 0x02 (changeset)]
//! [payload: remaining bytes, the raw sqlite-diff-rs changeset]
//! ```
//!
//! The 16-byte `msg_id` is per-edge: it lets receivers deduplicate
//! gossiped changesets that arrive along more than one path through the
//! mesh. There is only one kind of payload, raw session-extension
//! changeset bytes. Identity announcements ("Alice is here") and
//! ephemeral status events ("Alice is typing") both flow as
//! changesets to dedicated tables; the wire format does not have a
//! separate envelope for them.

use std::collections::{HashSet, VecDeque};

use uuid::Uuid;

const VERSION: u8 = 1;
const HEADER_LEN: usize = 1 + 16 + 1;

/// Maximum number of message IDs the dedup cache holds. When the cache
/// is full and a new ID arrives, the oldest is evicted.
pub const DEDUP_CAPACITY: usize = 256;

/// Type of frame carried in an envelope. The wire currently uses only
/// the single `Changeset` variant; the enum exists so that hypothetical
/// future kinds (e.g., compressed payloads) can be added without
/// breaking forward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// Raw sqlite-diff-rs changeset bytes.
    Changeset = 0x02,
}

impl Kind {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x02 => Some(Self::Changeset),
            _ => None,
        }
    }
}

/// A decoded frame borrowing into the source buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame<'a> {
    /// Sender-assigned UUID used to deduplicate gossiped messages.
    pub msg_id: Uuid,
    /// What kind of payload this frame carries.
    pub kind: Kind,
    /// Payload bytes (length implied by total frame length minus header).
    pub payload: &'a [u8],
}

/// Errors produced when decoding a frame.
#[derive(Debug)]
pub enum WireError {
    /// Frame was shorter than the 18-byte header.
    Truncated,
    /// Version byte did not match a version this build understands.
    UnsupportedVersion(u8),
    /// Kind byte did not match a known kind.
    UnknownKind(u8),
}

impl core::fmt::Display for WireError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Truncated => write!(f, "frame shorter than 18-byte header"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported wire version {v}"),
            Self::UnknownKind(k) => write!(f, "unknown frame kind {k:#x}"),
        }
    }
}

impl std::error::Error for WireError {}

/// Build a binary frame ready to push through the data channel.
#[must_use]
pub fn encode(kind: Kind, msg_id: Uuid, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(VERSION);
    out.extend_from_slice(msg_id.as_bytes());
    out.push(kind as u8);
    out.extend_from_slice(payload);
    out
}

/// Decode a frame previously produced by [`encode`]. Returns a borrowed
/// view into `bytes`.
///
/// # Errors
///
/// Returns [`WireError`] if the buffer is too short, declares an
/// unknown wire version, or carries an unknown frame kind.
pub fn decode(bytes: &[u8]) -> Result<Frame<'_>, WireError> {
    if bytes.len() < HEADER_LEN {
        return Err(WireError::Truncated);
    }
    let version = bytes[0];
    if version != VERSION {
        return Err(WireError::UnsupportedVersion(version));
    }
    let id_bytes: [u8; 16] = bytes[1..17].try_into().expect("17 - 1 == 16");
    let msg_id = Uuid::from_bytes(id_bytes);
    let kind = Kind::from_byte(bytes[17]).ok_or(WireError::UnknownKind(bytes[17]))?;
    let payload = &bytes[HEADER_LEN..];
    Ok(Frame {
        msg_id,
        kind,
        payload,
    })
}

/// Fixed-capacity LRU dedup cache for message IDs. Inserting an ID that
/// is already present is a no-op and returns `false`. Otherwise the ID
/// is recorded (evicting the oldest if at capacity) and the call
/// returns `true`.
#[derive(Default, Debug)]
pub struct DedupCache {
    order: VecDeque<Uuid>,
    set: HashSet<Uuid>,
}

impl DedupCache {
    /// Create an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to insert `id`. Returns `true` if the ID was new (and thus
    /// the caller should process the message), or `false` if it was
    /// already present (caller should drop the message).
    pub fn insert(&mut self, id: Uuid) -> bool {
        if !self.set.insert(id) {
            return false;
        }
        if self.order.len() >= DEDUP_CAPACITY
            && let Some(old) = self.order.pop_front()
        {
            self.set.remove(&old);
        }
        self.order.push_back(id);
        true
    }

    /// Returns the number of IDs currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Returns `true` if the cache holds no IDs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_changeset() {
        let id = Uuid::new_v4();
        let payload = (0u8..32).collect::<Vec<_>>();
        let bytes = encode(Kind::Changeset, id, &payload);
        let frame = decode(&bytes).expect("decode");
        assert_eq!(frame.msg_id, id);
        assert_eq!(frame.kind, Kind::Changeset);
        assert_eq!(frame.payload, payload.as_slice());
    }

    #[test]
    fn decode_rejects_short_buffer() {
        let bytes = [1u8; 10];
        assert!(matches!(decode(&bytes), Err(WireError::Truncated)));
    }

    #[test]
    fn decode_rejects_bad_version() {
        let id = Uuid::new_v4();
        let mut bytes = encode(Kind::Changeset, id, b"x");
        bytes[0] = 9;
        assert!(matches!(
            decode(&bytes),
            Err(WireError::UnsupportedVersion(9))
        ));
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        let id = Uuid::new_v4();
        let mut bytes = encode(Kind::Changeset, id, b"x");
        bytes[17] = 0xFF;
        assert!(matches!(decode(&bytes), Err(WireError::UnknownKind(0xFF))));
    }

    #[test]
    fn dedup_inserts_once() {
        let mut cache = DedupCache::new();
        let id = Uuid::new_v4();
        assert!(cache.insert(id));
        assert!(!cache.insert(id));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn dedup_evicts_oldest_at_capacity() {
        let mut cache = DedupCache::new();
        let mut ids = Vec::new();
        for _ in 0..DEDUP_CAPACITY {
            let id = Uuid::new_v4();
            ids.push(id);
            assert!(cache.insert(id));
        }
        assert!(!cache.insert(ids[0]));
        let extra = Uuid::new_v4();
        assert!(cache.insert(extra));
        assert!(cache.insert(ids[0]));
    }
}
