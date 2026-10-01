//! Structured benchmark results for JSON serialization.

use serde::Serialize;
use std::collections::BTreeMap;

/// Complete benchmark results.
#[derive(Debug, Serialize)]
pub struct BenchmarkResults {
    /// Metadata about the benchmark run.
    pub metadata: BenchmarkMetadata,
    /// Results for mixed workload at various batch sizes.
    pub mixed_workload: Vec<MixedWorkloadResult>,
}

/// Metadata about the benchmark configuration.
#[derive(Debug, Serialize)]
pub struct BenchmarkMetadata {
    /// Schema used for benchmarking.
    pub schema: String,
    /// Format names in order.
    pub formats: Vec<String>,
    /// Compressor names in order.
    pub compressors: Vec<String>,
    /// Number of iterations for timing measurements.
    pub timing_iterations: u32,
    /// Mixed workload distribution.
    pub workload_distribution: WorkloadDistribution,
}

/// Workload distribution percentages.
#[derive(Debug, Serialize)]
pub struct WorkloadDistribution {
    pub insert_pct: u32,
    pub update_pct: u32,
    pub delete_pct: u32,
}

/// Result for mixed workload at a specific batch size.
#[derive(Debug, Serialize)]
pub struct MixedWorkloadResult {
    /// Number of operations in the batch.
    pub num_operations: usize,
    /// Raw content size (theoretical minimum).
    pub raw_bytes: usize,
    /// Per-format results.
    pub formats: BTreeMap<String, FormatResult>,
}

/// Results for a single format at a specific batch size.
#[derive(Debug, Serialize)]
pub struct FormatResult {
    /// Uncompressed payload size in bytes.
    pub uncompressed_bytes: usize,
    /// Overhead vs raw content (uncompressed).
    pub overhead_pct: f64,
    /// Per-compressor results.
    pub compression: BTreeMap<String, CompressionResult>,
}

/// Results for a single compressor on a single format.
#[derive(Debug, Serialize)]
pub struct CompressionResult {
    /// Compressed payload size in bytes.
    pub compressed_bytes: usize,
    /// Compression ratio (compressed / uncompressed).
    pub compression_ratio: f64,
    /// Time to compress + decompress in microseconds (mean).
    pub compress_decompress_us: u64,
    /// Standard deviation of compress + decompress time in microseconds.
    pub compress_decompress_stddev_us: u64,
}

impl BenchmarkResults {
    /// Create a new empty results structure.
    pub fn new(formats: Vec<String>, compressors: Vec<String>, timing_iterations: u32) -> Self {
        Self {
            metadata: BenchmarkMetadata {
                schema: "messages(id BLOB PK, sender_id BLOB, receiver_id BLOB, body TEXT, created_at TEXT)".to_string(),
                formats,
                compressors,
                timing_iterations,
                workload_distribution: WorkloadDistribution {
                    insert_pct: 60,
                    update_pct: 25,
                    delete_pct: 15,
                },
            },
            mixed_workload: Vec::new(),
        }
    }

    /// Add a mixed workload result.
    ///
    /// `compression_data` is indexed as [format_idx][compressor_idx] -> (compressed_bytes, mean_us, stddev_us)
    pub fn add_mixed_workload(
        &mut self,
        num_operations: usize,
        raw_bytes: usize,
        uncompressed: Vec<usize>,
        compression_data: Vec<Vec<(usize, u64, u64)>>,
    ) {
        let mut formats = BTreeMap::new();
        for (i, format_name) in self.metadata.formats.iter().enumerate() {
            let overhead_pct = if raw_bytes > 0 {
                (uncompressed[i] as f64 - raw_bytes as f64) / raw_bytes as f64 * 100.0
            } else {
                0.0
            };

            let mut compression = BTreeMap::new();
            for (j, compressor_name) in self.metadata.compressors.iter().enumerate() {
                let (compressed_bytes, mean_us, stddev_us) = compression_data[i][j];
                compression.insert(
                    compressor_name.clone(),
                    CompressionResult {
                        compressed_bytes,
                        compression_ratio: compressed_bytes as f64 / uncompressed[i] as f64,
                        compress_decompress_us: mean_us,
                        compress_decompress_stddev_us: stddev_us,
                    },
                );
            }

            formats.insert(
                format_name.clone(),
                FormatResult {
                    uncompressed_bytes: uncompressed[i],
                    overhead_pct,
                    compression,
                },
            );
        }
        self.mixed_workload.push(MixedWorkloadResult {
            num_operations,
            raw_bytes,
            formats,
        });
    }

    /// Serialize to pretty JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("failed to serialize results")
    }
}
