//! Apply-benchmark report generator.
//!
//! Reads Criterion JSON results from `target/criterion/`, produces SVG charts
//! and a Markdown summary report.
//!
//! Run: `cargo run -p apply-bench-report [-- <criterion_dir> <output_dir>]`

mod data;
mod markdown;
mod plots;

use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let criterion_dir = if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        // Default: workspace root's target/criterion
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/criterion")
    };

    let output_dir = if args.len() > 2 {
        PathBuf::from(&args[2])
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("plots")
    };

    let criterion_dir = criterion_dir.canonicalize().unwrap_or_else(|e| {
        eprintln!(
            "error: cannot resolve criterion directory '{}': {e}",
            criterion_dir.display()
        );
        std::process::exit(1);
    });

    eprintln!("Reading from: {}", criterion_dir.display());
    eprintln!("Output to:    {}", output_dir.display());

    let results = data::ResultSet::load(&criterion_dir);

    if results.results.is_empty() {
        eprintln!(
            "error: no benchmark results found in {}",
            criterion_dir.display()
        );
        std::process::exit(1);
    }

    // Validate: report expected vs found benchmarks.
    let apply_count = results.apply_results().len();
    let gen_count = results.generation_results().len();
    eprintln!(
        "Found {} apply benchmarks, {} generation benchmarks",
        apply_count, gen_count,
    );

    // Expected: 4 methods × (2 pks × 2 states × 3 op_counts + 2 pks × 3 variants) = 4 × (12+6) = 72
    // + 4 methods × (2 pks × 1000 base already counted) → total 80 apply benchmarks
    let expected_apply = 4 * (2 * 2 * 3 + 2 * 3); // 72
    if apply_count < expected_apply {
        eprintln!(
            "warning: expected at least {expected_apply} apply benchmarks, found {apply_count}"
        );
    }

    // Generate charts.
    if let Err(e) = plots::generate_all(&results, &output_dir) {
        eprintln!("error generating charts: {e}");
        std::process::exit(1);
    }

    // Generate Markdown report.
    if let Err(e) = markdown::generate_report(&results, &output_dir) {
        eprintln!("error generating report: {e}");
        std::process::exit(1);
    }

    eprintln!(
        "\nDone! Report at: {}",
        output_dir.join("report.md").display()
    );
}
