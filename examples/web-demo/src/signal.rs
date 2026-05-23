//! URL-fragment signaling for the WebRTC handshake.
//!
//! Each peer compresses its mDNS-stripped SDP with DEFLATE (via
//! `miniz_oxide`) and encodes the bytes as base64url. The resulting
//! string is prefixed with `o=` for an offer (carried in a full URL,
//! since the answerer navigates to it) or shipped bare for an answer
//! (since the offerer pastes it into a text box).
//!
//! DEFLATE shrinks a typical 750-byte data-channel SDP to ~480 bytes,
//! and after base64url the URL fragment is ~640 chars instead of ~1000
//! (a 34% reduction). Brotli and zstd compress slightly better, but
//! their wasm cost is ~5x to ~10x larger, which is not worth the extra
//! few percent at this size class.
//!
//! The fragment never leaves the browser (URLs `#...` are not sent to
//! the server), so this works with any static host and needs no
//! infrastructure beyond a public STUN server.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// A decoded fragment carrying either an offer or an answer SDP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded {
    /// Offer from the remote peer; we should answer.
    Offer(String),
    /// Answer from the remote peer; we should set it as remote description.
    Answer(String),
}

/// Errors that can arise when parsing a fragment or URL.
#[derive(Debug)]
pub enum SignalError {
    /// The string did not contain a recognizable fragment.
    NoFragment,
    /// Fragment did not start with `o=` or `a=`.
    UnknownPrefix,
    /// base64url decoding failed.
    InvalidBase64,
    /// DEFLATE inflation failed.
    InvalidCompressed,
    /// Decoded payload was not valid UTF-8 SDP.
    InvalidUtf8,
}

impl core::fmt::Display for SignalError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NoFragment => write!(f, "URL has no fragment"),
            Self::UnknownPrefix => write!(f, "fragment must start with o= (offer) or a= (answer)"),
            Self::InvalidBase64 => write!(f, "fragment payload is not valid base64url"),
            Self::InvalidCompressed => write!(f, "fragment payload is not valid DEFLATE data"),
            Self::InvalidUtf8 => write!(f, "decoded SDP is not valid UTF-8"),
        }
    }
}

impl std::error::Error for SignalError {}

/// Decode a full URL or bare fragment into its SDP. Accepts inputs with or
/// without a leading `#`, so callers can pass a copy-pasted URL directly.
///
/// # Errors
///
/// Returns [`SignalError`] if the input is empty, has no recognizable
/// prefix, or fails base64url or UTF-8 decoding.
pub fn decode(url_or_fragment: &str) -> Result<Decoded, SignalError> {
    let body = url_or_fragment
        .split_once('#')
        .map_or(url_or_fragment, |(_, frag)| frag)
        .trim_start_matches('#')
        .trim();

    if body.is_empty() {
        return Err(SignalError::NoFragment);
    }

    if let Some(blob) = body.strip_prefix("o=") {
        Ok(Decoded::Offer(decode_blob(blob)?))
    } else if let Some(blob) = body.strip_prefix("a=") {
        Ok(Decoded::Answer(decode_blob(blob)?))
    } else {
        Err(SignalError::UnknownPrefix)
    }
}

/// Build a complete offer URL by appending the encoded SDP to the page's
/// current origin + path.
#[must_use]
pub fn encode_offer_url(sdp: &str) -> String {
    format!("{}#o={}", base_url(), encode_blob(sdp))
}

/// Encode an answer SDP as a bare base64url blob (no URL wrapping). The
/// offerer never needs to navigate to the answer, they paste it into a
/// text box on the existing tab, so stripping the URL prefix saves a
/// couple dozen characters of copy-paste.
#[must_use]
pub fn encode_answer_blob(sdp: &str) -> String {
    encode_blob(sdp)
}

/// Decode an answer blob produced by [`encode_answer_blob`]. Accepts the
/// bare base64url payload, optionally prefixed by `a=` or `#a=` to
/// tolerate paste-arounds.
///
/// # Errors
///
/// Returns [`SignalError`] if the input is empty or fails base64url/UTF-8
/// decoding.
pub fn decode_answer_blob(input: &str) -> Result<String, SignalError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(SignalError::NoFragment);
    }
    let body = trimmed.strip_prefix('#').unwrap_or(trimmed);
    let body = body.strip_prefix("a=").unwrap_or(body);
    decode_blob(body)
}

/// Inspect the page's current URL for an offer or answer fragment.
///
/// Returns `Ok(None)` when there is no fragment, `Ok(Some(_))` when one
/// is present and decodes cleanly, or an error if the fragment is malformed.
///
/// # Errors
///
/// Returns [`SignalError`] if a fragment is present but cannot be decoded.
pub fn fragment_from_url() -> Result<Option<Decoded>, SignalError> {
    let Some(win) = web_sys::window() else {
        return Ok(None);
    };
    let hash = win.location().hash().unwrap_or_default();
    if hash.is_empty() || hash == "#" {
        return Ok(None);
    }
    decode(&hash).map(Some)
}

fn base_url() -> String {
    let win = web_sys::window().expect("window");
    let loc = win.location();
    let origin = loc.origin().unwrap_or_default();
    let path = loc.pathname().unwrap_or_else(|_| "/".into());
    format!("{origin}{path}")
}

fn encode_blob(sdp: &str) -> String {
    let compressed = miniz_oxide::deflate::compress_to_vec(sdp.as_bytes(), 10);
    URL_SAFE_NO_PAD.encode(&compressed)
}

fn decode_blob(blob: &str) -> Result<String, SignalError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(blob.as_bytes())
        .map_err(|_| SignalError::InvalidBase64)?;
    let inflated = miniz_oxide::inflate::decompress_to_vec(&bytes)
        .map_err(|_| SignalError::InvalidCompressed)?;
    String::from_utf8(inflated).map_err(|_| SignalError::InvalidUtf8)
}
