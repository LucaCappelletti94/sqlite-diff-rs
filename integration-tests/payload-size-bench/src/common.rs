//! Shared types, test data generation, and the `Format` trait.

use chrono::{TimeZone, Utc};
use flate2::Compression;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use rand::{RngExt, rng};
use sqlite_diff_rs::TableSchema;
use std::io::{Read, Write};
use std::sync::OnceLock;
use uuid::{NoContext, Timestamp, Uuid};

// ---------------------------------------------------------------------------
// Chat schema
// ---------------------------------------------------------------------------

/// messages (id BLOB PK, sender_id BLOB, receiver_id BLOB, body TEXT, created_at TEXT)
pub fn messages_schema() -> TableSchema {
    TableSchema::new("messages".into(), 5, vec![1, 0, 0, 0, 0])
}

// ---------------------------------------------------------------------------
// Test data loaded from CSV
// ---------------------------------------------------------------------------

/// A message pair from the dataset (prompt + response).
#[derive(Clone)]
struct ChatEntry {
    prompt: String,
    response: String,
}

/// Lazily loaded dataset from data/dataset.csv.
fn load_dataset() -> &'static Vec<ChatEntry> {
    static DATASET: OnceLock<Vec<ChatEntry>> = OnceLock::new();
    DATASET.get_or_init(|| {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/data/dataset.csv");
        let mut reader = csv::Reader::from_path(path).expect("Failed to open dataset.csv");
        reader
            .records()
            .map(|r| {
                let record = r.expect("Invalid CSV record");
                ChatEntry {
                    prompt: record.get(0).unwrap_or("").to_string(),
                    response: record.get(1).unwrap_or("").to_string(),
                }
            })
            .collect()
    })
}

pub struct TestMessage {
    pub id: Uuid,
    pub sender: Uuid,
    pub receiver: Uuid,
    pub body: String,
    pub update_body: String,
    pub created_at: String,
}

/// Generate n messages using real chat data from the dataset.
/// Uses UUID v7 with timestamps distributed between 2000 and 2020.
pub fn generate_messages(n: usize) -> Vec<TestMessage> {
    let dataset = load_dataset();
    let dataset_len = dataset.len();

    // Time range: 2000-01-01 to 2020-01-01
    let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let start_secs = start.timestamp() as u64;
    let end_secs = end.timestamp() as u64;
    let range_secs = end_secs - start_secs;

    // Generate 50 user IDs with v7 UUIDs spread across time range
    let mut rng = rng();
    let num_users: usize = 50;
    let users: Vec<Uuid> = (0..num_users)
        .map(|i| {
            let user_secs = start_secs + (i as u64 * range_secs) / (num_users as u64);
            Uuid::new_v7(Timestamp::from_unix(NoContext, user_secs, 0))
        })
        .collect();

    let mut messages = Vec::with_capacity(n);
    for i in 0..n {
        // Cycle through dataset entries
        let entry = &dataset[i % dataset_len];
        // Next entry for update_body
        let next_entry = &dataset[(i + 1) % dataset_len];

        // Randomly select sender and receiver from user pool
        let sender_idx = rng.random_range(0..num_users);
        let receiver_idx = (sender_idx + 1 + rng.random_range(0..num_users - 1)) % num_users;
        let sender = users[sender_idx];
        let receiver = users[receiver_idx];

        // Distribute timestamps evenly across 2000-2020
        let msg_secs = start_secs + (i as u64 * range_secs) / (n.max(1) as u64);
        let msg_ts = Timestamp::from_unix(NoContext, msg_secs, 0);
        let msg_dt = Utc.timestamp_opt(msg_secs as i64, 0).unwrap();
        let created_at = msg_dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

        messages.push(TestMessage {
            id: Uuid::new_v7(msg_ts),
            sender,
            receiver,
            body: entry.prompt.clone(),
            update_body: next_entry.response.clone(),
            created_at,
        });
    }
    messages
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Hex-encode a byte slice (for SQL/JSON blob encoding).
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Raw content size — the actual data bytes with zero framing.
/// IDs are 16-byte UUIDs stored as binary.
pub fn raw_content_message(m: &TestMessage) -> usize {
    16 * 3 + m.body.len() + m.created_at.len()
}

// ---------------------------------------------------------------------------
// Compression algorithms
// ---------------------------------------------------------------------------

/// A compression algorithm with compress/decompress operations.
pub trait Compressor: Send + Sync {
    fn name(&self) -> &'static str;
    fn compress(&self, data: &[u8]) -> Vec<u8>;
    fn decompress(&self, data: &[u8]) -> Vec<u8>;
}

/// DEFLATE at level 6 (WebSocket permessage-deflate default).
pub struct DeflateCompressor;

impl Compressor for DeflateCompressor {
    fn name(&self) -> &'static str {
        "DEFLATE"
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(6));
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        let mut decoder = DeflateDecoder::new(data);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result).unwrap();
        result
    }
}

/// Zstandard at default level (3).
pub struct ZstdCompressor;

impl Compressor for ZstdCompressor {
    fn name(&self) -> &'static str {
        "Zstd"
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        zstd::encode_all(data, 3).unwrap()
    }

    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        zstd::decode_all(data).unwrap()
    }
}

/// LZ4 (block mode, high compression).
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn name(&self) -> &'static str {
        "LZ4"
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        lz4_flex::compress_prepend_size(data)
    }

    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        lz4_flex::decompress_size_prepended(data).unwrap()
    }
}

/// All available compressors.
pub fn all_compressors() -> Vec<Box<dyn Compressor>> {
    vec![
        Box::new(DeflateCompressor),
        Box::new(ZstdCompressor),
        Box::new(Lz4Compressor),
    ]
}

// ---------------------------------------------------------------------------
// Format trait — each serialization format implements this
// ---------------------------------------------------------------------------

/// A serialization format that can encode INSERT, UPDATE, DELETE operations
/// for the messages table, both individually and as a batch.
pub trait Format {
    /// Human-readable name (e.g. "SQL", "JSON", "Patchset").
    fn name(&self) -> &'static str;

    /// Serialize a mixed batch and return the raw bytes.
    ///
    /// `inserts`, `updates`, `deletes` are slices into the same messages vec.
    /// Each message has its own `update_body` field for update operations.
    fn batch_mixed(
        &self,
        inserts: &[TestMessage],
        updates: &[TestMessage],
        deletes: &[TestMessage],
    ) -> Vec<u8>;
}
