//! Payload size comparison: SQL vs JSON vs MsgPack vs CBOR vs Protobuf vs Patchset vs Changeset.
//!
//! Benchmarks multiple compressors: DEFLATE (WebSocket), Zstd, LZ4.
//!
//! Outputs structured JSON to stdout. Also saves to plots/results.json and generates SVG plots.
//!
//! Run: `cargo run -p payload-size-bench`

mod binary_serde;
mod common;
mod format_cbor;
mod format_changeset;
mod format_json;
mod format_msgpack;
mod format_patchset;
mod format_protobuf;
mod format_sql;
mod plots;
mod results;

use results::BenchmarkResults;

use std::path::Path;
use std::time::Instant;

use common::{Compressor, Format, all_compressors, generate_messages, raw_content_message};

// ---------------------------------------------------------------------------
// All formats in display order
// ---------------------------------------------------------------------------

fn all_formats() -> Vec<Box<dyn Format>> {
    vec![
        Box::new(format_sql::Sql),
        Box::new(format_json::Json),
        Box::new(format_msgpack::MsgPack),
        Box::new(format_cbor::Cbor),
        Box::new(format_protobuf::Protobuf),
        Box::new(format_patchset::Patchset),
        Box::new(format_changeset::Changeset),
    ]
}

/// Number of iterations for timing measurements.
const TIMING_ITERATIONS: u32 = 100;

// ---------------------------------------------------------------------------
// Data collection
// ---------------------------------------------------------------------------

/// Returns (raw_bytes, uncompressed_per_format, compression_data)
/// compression_data[format_idx][compressor_idx] = (compressed_bytes, mean_us, stddev_us)
fn collect_mixed_workload(
    n: usize,
    formats: &[Box<dyn Format>],
    compressors: &[Box<dyn Compressor>],
) -> (usize, Vec<usize>, Vec<Vec<(usize, u64, u64)>>) {
    let iterations = 5;

    let msgs = generate_messages(n);
    let n_insert = (n * 60) / 100;
    let n_update = (n * 25) / 100;
    let n_delete = n - n_insert - n_update;

    let inserts = &msgs[..n_insert];
    let updates = &msgs[n_insert..n_insert + n_update];
    let deletes = &msgs[n_insert + n_update..];

    let mut uncompressed = Vec::with_capacity(formats.len());
    let mut compression_data = Vec::with_capacity(formats.len());

    for fmt in formats.iter() {
        let data = fmt.batch_mixed(inserts, updates, deletes);
        uncompressed.push(data.len());

        let mut compressor_results = Vec::with_capacity(compressors.len());
        for comp in compressors.iter() {
            // Measure compress + decompress time over multiple iterations
            let mut times_us = Vec::with_capacity(iterations);
            for _ in 0..iterations {
                let start = Instant::now();
                let compressed = comp.compress(&data);
                let _decompressed = comp.decompress(&compressed);
                times_us.push(start.elapsed().as_micros() as f64);
            }

            // Compute mean and std dev
            let mean = times_us.iter().sum::<f64>() / times_us.len() as f64;
            let variance =
                times_us.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / times_us.len() as f64;
            let stddev = variance.sqrt();

            // Get compressed size
            let compressed_bytes = comp.compress(&data).len();

            compressor_results.push((compressed_bytes, mean as u64, stddev as u64));
        }
        compression_data.push(compressor_results);
    }

    // Raw content: 16-byte binary UUIDs + text content
    let raw_inserts: usize = inserts.iter().map(raw_content_message).sum();
    let raw_updates: usize = updates.iter().map(|m| 16 + m.update_body.len()).sum();
    let raw_deletes: usize = n_delete * 16;
    let raw_bytes = raw_inserts + raw_updates + raw_deletes;

    (raw_bytes, uncompressed, compression_data)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let formats = all_formats();
    let compressors = all_compressors();

    let format_names: Vec<String> = formats.iter().map(|f| f.name().to_string()).collect();
    let compressor_names: Vec<String> = compressors.iter().map(|c| c.name().to_string()).collect();

    // Build structured results
    let mut results = BenchmarkResults::new(
        format_names.clone(),
        compressor_names.clone(),
        TIMING_ITERATIONS,
    );

    // =====================================================================
    // Mixed workload at scale
    // =====================================================================

    let batch_sizes = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000];
    for &n in &batch_sizes {
        let (raw_bytes, uncompressed, compression_data) =
            collect_mixed_workload(n, &formats, &compressors);
        results.add_mixed_workload(n, raw_bytes, uncompressed, compression_data);
    }

    // =====================================================================
    // Output
    // =====================================================================

    // Save JSON and output to stdout
    let json = results.to_json();
    println!("{json}");

    // Also save to file and generate plots
    let plot_dir = Path::new("plots");
    std::fs::create_dir_all(plot_dir).ok();
    std::fs::write(plot_dir.join("results.json"), &json).ok();

    // Generate SVG plots
    let format_slice: Vec<&str> = formats.iter().map(|f| f.name()).collect();

    // Uncompressed payload sizes
    let ops_data: Vec<(f64, Vec<f64>)> = results
        .mixed_workload
        .iter()
        .map(|r| {
            let bytes: Vec<f64> = format_slice
                .iter()
                .map(|name| {
                    r.formats
                        .get(*name)
                        .map(|f| f.uncompressed_bytes as f64)
                        .unwrap_or(0.0)
                })
                .collect();
            (r.num_operations as f64, bytes)
        })
        .collect();

    plots::line_chart(
        &ops_data,
        &format_slice,
        "Number of Operations",
        "Total Bytes (Uncompressed)",
        &plot_dir.join("ops_scaling.svg"),
    )
    .ok();

    // Per-compressor: stacked size + time charts
    for comp in &compressors {
        let comp_name = comp.name();
        let safe_name = comp_name.to_lowercase();

        // Compressed payload sizes
        let compressed_data: Vec<(f64, Vec<f64>)> = results
            .mixed_workload
            .iter()
            .map(|r| {
                let bytes: Vec<f64> = format_slice
                    .iter()
                    .map(|name| {
                        r.formats
                            .get(*name)
                            .and_then(|f| f.compression.get(comp_name))
                            .map(|c| c.compressed_bytes as f64)
                            .unwrap_or(0.0)
                    })
                    .collect();
                (r.num_operations as f64, bytes)
            })
            .collect();

        // Timing with error bars
        let time_data: Vec<(f64, Vec<f64>, Vec<f64>)> = results
            .mixed_workload
            .iter()
            .map(|r| {
                let times: Vec<f64> = format_slice
                    .iter()
                    .map(|name| {
                        r.formats
                            .get(*name)
                            .and_then(|f| f.compression.get(comp_name))
                            .map(|c| c.compress_decompress_us as f64)
                            .unwrap_or(0.0)
                    })
                    .collect();
                let stddevs: Vec<f64> = format_slice
                    .iter()
                    .map(|name| {
                        r.formats
                            .get(*name)
                            .and_then(|f| f.compression.get(comp_name))
                            .map(|c| c.compress_decompress_stddev_us as f64)
                            .unwrap_or(0.0)
                    })
                    .collect();
                (r.num_operations as f64, times, stddevs)
            })
            .collect();

        // Stacked chart: size on top, time on bottom
        plots::stacked_size_and_time(
            &compressed_data,
            &time_data,
            &format_slice,
            "Number of Operations",
            &format!("{} — Compressed Size & Timing", comp_name),
            &plot_dir.join(format!("{}.svg", safe_name)),
        )
        .ok();
    }

    // Compressor comparison chart: for Patchset format, show all compressors
    let patchset_size_comparison: Vec<(f64, Vec<f64>)> = results
        .mixed_workload
        .iter()
        .map(|r| {
            let bytes: Vec<f64> = compressor_names
                .iter()
                .map(|comp_name| {
                    r.formats
                        .get("Patchset")
                        .and_then(|f| f.compression.get(comp_name))
                        .map(|c| c.compressed_bytes as f64)
                        .unwrap_or(0.0)
                })
                .collect();
            (r.num_operations as f64, bytes)
        })
        .collect();

    let patchset_time_comparison: Vec<(f64, Vec<f64>, Vec<f64>)> = results
        .mixed_workload
        .iter()
        .map(|r| {
            let times: Vec<f64> = compressor_names
                .iter()
                .map(|comp_name| {
                    r.formats
                        .get("Patchset")
                        .and_then(|f| f.compression.get(comp_name))
                        .map(|c| c.compress_decompress_us as f64)
                        .unwrap_or(0.0)
                })
                .collect();
            let stddevs: Vec<f64> = compressor_names
                .iter()
                .map(|comp_name| {
                    r.formats
                        .get("Patchset")
                        .and_then(|f| f.compression.get(comp_name))
                        .map(|c| c.compress_decompress_stddev_us as f64)
                        .unwrap_or(0.0)
                })
                .collect();
            (r.num_operations as f64, times, stddevs)
        })
        .collect();

    let compressor_slice: Vec<&str> = compressor_names.iter().map(|s| s.as_str()).collect();
    plots::stacked_size_and_time(
        &patchset_size_comparison,
        &patchset_time_comparison,
        &compressor_slice,
        "Number of Operations",
        "Patchset — Compressor Comparison",
        &plot_dir.join("compressor_comparison_patchset.svg"),
    )
    .ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that exercises all format and compressor code paths.
    /// Uses small batch sizes to keep test fast while ensuring coverage.
    #[test]
    fn test_all_formats_and_compressors() {
        let formats = all_formats();
        let compressors = all_compressors();

        // Small batch sizes to exercise the code paths
        for n in [1, 5, 10] {
            let (raw_bytes, uncompressed, compression_data) =
                collect_mixed_workload(n, &formats, &compressors);

            assert!(raw_bytes > 0, "raw_bytes should be non-zero for n={n}");
            assert_eq!(
                uncompressed.len(),
                formats.len(),
                "should have one entry per format"
            );
            assert_eq!(
                compression_data.len(),
                formats.len(),
                "should have compression data per format"
            );

            for (fmt_idx, fmt) in formats.iter().enumerate() {
                assert!(
                    uncompressed[fmt_idx] > 0,
                    "uncompressed size should be > 0 for {} at n={n}",
                    fmt.name()
                );
                assert_eq!(
                    compression_data[fmt_idx].len(),
                    compressors.len(),
                    "should have data per compressor for {}",
                    fmt.name()
                );

                for (comp_idx, comp) in compressors.iter().enumerate() {
                    let (compressed_bytes, _mean_us, _stddev_us) =
                        compression_data[fmt_idx][comp_idx];
                    assert!(
                        compressed_bytes > 0,
                        "compressed size should be > 0 for {} with {} at n={n}",
                        fmt.name(),
                        comp.name()
                    );
                }
            }
        }
    }

    /// Test BenchmarkResults serialization.
    #[test]
    fn test_results_serialization() {
        let formats = all_formats();
        let compressors = all_compressors();

        let format_names: Vec<String> = formats.iter().map(|f| f.name().to_string()).collect();
        let compressor_names: Vec<String> =
            compressors.iter().map(|c| c.name().to_string()).collect();

        let mut results = BenchmarkResults::new(format_names, compressor_names, TIMING_ITERATIONS);

        let (raw_bytes, uncompressed, compression_data) =
            collect_mixed_workload(5, &formats, &compressors);
        results.add_mixed_workload(5, raw_bytes, uncompressed, compression_data);

        let json = results.to_json();
        assert!(!json.is_empty(), "JSON output should not be empty");
        assert!(
            json.contains("mixed_workload"),
            "JSON should contain mixed_workload"
        );
    }

    /// Test plot generation (to temp directory).
    #[test]
    fn test_plot_generation() {
        let formats = all_formats();
        let format_slice: Vec<&str> = formats.iter().map(|f| f.name()).collect();

        let temp_dir = std::env::temp_dir().join("payload-size-bench-test");
        std::fs::create_dir_all(&temp_dir).ok();

        // Simple data for plot
        let ops_data: Vec<(f64, Vec<f64>)> = vec![
            (1.0, vec![100.0; format_slice.len()]),
            (5.0, vec![500.0; format_slice.len()]),
        ];

        let result = plots::line_chart(
            &ops_data,
            &format_slice,
            "X Label",
            "Y Label",
            &temp_dir.join("test_plot.svg"),
        );
        assert!(result.is_ok(), "plot generation should succeed");

        // Test stacked chart
        let time_data: Vec<(f64, Vec<f64>, Vec<f64>)> = vec![
            (
                1.0,
                vec![10.0; format_slice.len()],
                vec![1.0; format_slice.len()],
            ),
            (
                5.0,
                vec![50.0; format_slice.len()],
                vec![5.0; format_slice.len()],
            ),
        ];

        let result = plots::stacked_size_and_time(
            &ops_data,
            &time_data,
            &format_slice,
            "X Label",
            "Title",
            &temp_dir.join("test_stacked.svg"),
        );
        assert!(result.is_ok(), "stacked plot generation should succeed");

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }
}
