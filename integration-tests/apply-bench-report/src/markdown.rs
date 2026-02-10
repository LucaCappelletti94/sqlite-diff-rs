//! Markdown report generation for apply-benchmark results.
//!
//! Produces `report.md` with summary tables, embedded SVG references,
//! speedup ratios, and auto-generated key findings.

use std::fmt::Write;
use std::path::Path;

use crate::data::ResultSet;
use crate::plots::{METHOD_LABELS, METHODS};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format microseconds as a human-readable string.
fn fmt_us(us: f64) -> String {
    if us >= 1_000.0 {
        format!("{:.2} ms", us / 1_000.0)
    } else {
        format!("{:.1} µs", us)
    }
}

/// Compute speedup: `baseline / target`.
fn speedup(baseline_us: f64, target_us: f64) -> f64 {
    if target_us > 0.0 {
        baseline_us / target_us
    } else {
        f64::NAN
    }
}

// ---------------------------------------------------------------------------
// Report sections
// ---------------------------------------------------------------------------

fn write_header(out: &mut String) {
    writeln!(out, "# Apply Benchmark Report\n").unwrap();
    writeln!(out, "## Methodology\n").unwrap();
    writeln!(
        out,
        "This report compares four methods for applying changes to an SQLite database:\n"
    )
    .unwrap();
    writeln!(out, "| Method | Description |").unwrap();
    writeln!(out, "|--------|-------------|").unwrap();
    writeln!(
        out,
        "| **SQL (autocommit)** | Execute raw SQL statements one at a time (implicit autocommit) |"
    )
    .unwrap();
    writeln!(
        out,
        "| **SQL (transaction)** | Same SQL wrapped in a single `BEGIN…COMMIT` transaction |"
    )
    .unwrap();
    writeln!(
        out,
        "| **Patchset** | Apply a binary patchset via `conn.apply_strm()` |"
    )
    .unwrap();
    writeln!(
        out,
        "| **Changeset** | Apply a binary changeset via `conn.apply_strm()` |"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "All times are from Criterion.rs ({} confidence level). \
         Median is used as the primary metric. \
         Benchmarks run on in-memory SQLite databases.\n",
        "95%"
    )
    .unwrap();
}

fn write_summary_table(out: &mut String, results: &ResultSet) {
    writeln!(out, "## Summary Table\n").unwrap();
    writeln!(
        out,
        "| PK Type | State | Ops | Config | {} |",
        METHOD_LABELS.join(" | ")
    )
    .unwrap();
    writeln!(
        out,
        "|---------|-------|-----|--------|{}",
        METHOD_LABELS
            .iter()
            .map(|_| "------|")
            .collect::<Vec<_>>()
            .join("")
    )
    .unwrap();

    let groups = results.scaling_groups();
    for ((pk, state, config), ops_map) in &groups {
        for (ops, methods_map) in ops_map {
            // Find the fastest method for bolding.
            let fastest_median = methods_map
                .values()
                .map(|r| r.median_us())
                .fold(f64::MAX, f64::min);

            let mut row = format!("| {pk} | {state} | {ops} | {config} |");
            for &method in METHODS {
                if let Some(r) = methods_map.get(method) {
                    let med = r.median_us();
                    let sd = r.std_dev_us();
                    let cell = format!("{} ± {}", fmt_us(med), fmt_us(sd));
                    if (med - fastest_median).abs() < 0.01 {
                        write!(row, " **{cell}** |").unwrap();
                    } else {
                        write!(row, " {cell} |").unwrap();
                    }
                } else {
                    write!(row, " — |").unwrap();
                }
            }
            writeln!(out, "{row}").unwrap();
        }
    }
    writeln!(out).unwrap();
}

fn write_scaling_section(out: &mut String) {
    writeln!(out, "## Scaling Analysis\n").unwrap();
    writeln!(
        out,
        "How each apply method scales as the number of operations increases (30 → 100 → 1000).\n"
    )
    .unwrap();

    for pk in &["int_pk", "uuid_pk"] {
        for state in &["empty", "populated"] {
            let filename = format!("scaling_{pk}_{state}.svg");
            writeln!(out, "### {}, {}\n", pk.replace('_', " "), state).unwrap();
            writeln!(out, "![Scaling {pk} {state}]({filename})\n").unwrap();
        }
    }
}

fn write_method_comparison(out: &mut String, results: &ResultSet) {
    writeln!(out, "## Method Comparison (populated/1000, base config)\n").unwrap();

    for pk in &["int_pk", "uuid_pk"] {
        let filename = format!("method_{pk}.svg");
        writeln!(out, "### {}\n", pk.replace('_', " ")).unwrap();
        writeln!(out, "![Method comparison {pk}]({filename})\n").unwrap();

        // Speedup table.
        if let Some(sql_r) = results.find_apply(pk, "populated", 1000, "base", "sql") {
            let sql_us = sql_r.median_us();
            writeln!(out, "| Method | Median | Speedup vs SQL (autocommit) |").unwrap();
            writeln!(out, "|--------|--------|----------------------------|").unwrap();
            for &method in METHODS {
                if let Some(r) = results.find_apply(pk, "populated", 1000, "base", method) {
                    let med = r.median_us();
                    let sp = speedup(sql_us, med);
                    let label = crate::plots::METHOD_LABELS
                        [METHODS.iter().position(|&m| m == method).unwrap()];
                    writeln!(out, "| {label} | {} | {sp:.2}× |", fmt_us(med)).unwrap();
                }
            }
            writeln!(out).unwrap();
        }
    }
}

fn write_config_section(out: &mut String, results: &ResultSet) {
    writeln!(out, "## Configuration Variant Impact\n").unwrap();
    writeln!(
        out,
        "How secondary indexes, triggers, and foreign keys affect apply performance \
         (populated/1000 scenario).\n"
    )
    .unwrap();

    for pk in &["int_pk", "uuid_pk"] {
        let filename = format!("config_{pk}.svg");
        writeln!(out, "### {}\n", pk.replace('_', " ")).unwrap();
        writeln!(out, "![Config variants {pk}]({filename})\n").unwrap();

        // Overhead table.
        writeln!(out, "| Method | base | indexed | triggers | fk |").unwrap();
        writeln!(out, "|--------|------|---------|----------|----|").unwrap();
        for (mi, &method) in METHODS.iter().enumerate() {
            let label = METHOD_LABELS[mi];
            let base = results.find_apply(pk, "populated", 1000, "base", method);
            let mut row = format!("| {label} |");
            for cfg in &["base", "indexed", "triggers", "fk"] {
                if let Some(r) = results.find_apply(pk, "populated", 1000, cfg, method) {
                    let med = r.median_us();
                    if let Some(base_r) = base {
                        let pct = ((med / base_r.median_us()) - 1.0) * 100.0;
                        if *cfg == "base" {
                            write!(row, " {} |", fmt_us(med)).unwrap();
                        } else {
                            write!(row, " {} ({:+.1}%) |", fmt_us(med), pct).unwrap();
                        }
                    } else {
                        write!(row, " {} |", fmt_us(med)).unwrap();
                    }
                } else {
                    write!(row, " — |").unwrap();
                }
            }
            writeln!(out, "{row}").unwrap();
        }
        writeln!(out).unwrap();
    }
}

fn write_pk_section(out: &mut String, results: &ResultSet) {
    writeln!(out, "## Primary Key Type Impact\n").unwrap();
    writeln!(
        out,
        "Comparison of INTEGER PK vs UUID BLOB PK (populated/1000, base config).\n"
    )
    .unwrap();
    writeln!(out, "![PK comparison](pk_comparison.svg)\n").unwrap();

    writeln!(out, "| Method | int_pk | uuid_pk | Δ% |").unwrap();
    writeln!(out, "|--------|--------|---------|------|").unwrap();
    for (mi, &method) in METHODS.iter().enumerate() {
        let label = METHOD_LABELS[mi];
        let int_r = results.find_apply("int_pk", "populated", 1000, "base", method);
        let uuid_r = results.find_apply("uuid_pk", "populated", 1000, "base", method);
        match (int_r, uuid_r) {
            (Some(ir), Some(ur)) => {
                let pct = ((ur.median_us() / ir.median_us()) - 1.0) * 100.0;
                writeln!(
                    out,
                    "| {label} | {} | {} | {:+.1}% |",
                    fmt_us(ir.median_us()),
                    fmt_us(ur.median_us()),
                    pct
                )
                .unwrap();
            }
            _ => {
                writeln!(out, "| {label} | — | — | — |").unwrap();
            }
        }
    }
    writeln!(out).unwrap();
}

fn write_generation_section(out: &mut String, results: &ResultSet) {
    let gen_results = results.generation_results();
    if gen_results.is_empty() {
        return;
    }

    writeln!(out, "## Generation Benchmarks\n").unwrap();
    writeln!(
        out,
        "Time to generate a changeset/patchset from a database diff.\n"
    )
    .unwrap();

    writeln!(out, "| Benchmark | Method | Median | Std Dev |").unwrap();
    writeln!(out, "|-----------|--------|--------|---------|").unwrap();
    for r in &gen_results {
        writeln!(
            out,
            "| {} | {} | {} | {} |",
            r.group_id,
            r.function_id,
            fmt_us(r.median_us()),
            fmt_us(r.std_dev_us()),
        )
        .unwrap();
    }
    writeln!(out).unwrap();
}

fn write_key_findings(out: &mut String, results: &ResultSet) {
    writeln!(out, "## Key Findings\n").unwrap();

    // Compare changeset vs SQL autocommit at populated/1000/base.
    for pk in &["int_pk", "uuid_pk"] {
        let sql = results.find_apply(pk, "populated", 1000, "base", "sql");
        let changeset = results.find_apply(pk, "populated", 1000, "base", "changeset");
        let patchset = results.find_apply(pk, "populated", 1000, "base", "patchset");
        let sql_tx = results.find_apply(pk, "populated", 1000, "base", "sql_tx");

        if let (Some(s), Some(c)) = (sql, changeset) {
            let sp = speedup(s.median_us(), c.median_us());
            writeln!(
                out,
                "- **{pk}**: Changeset apply is **{sp:.1}×** faster than autocommit SQL at 1000 ops",
            )
            .unwrap();
        }
        if let (Some(s), Some(p)) = (sql, patchset) {
            let sp = speedup(s.median_us(), p.median_us());
            writeln!(
                out,
                "- **{pk}**: Patchset apply is **{sp:.1}×** faster than autocommit SQL at 1000 ops",
            )
            .unwrap();
        }
        if let (Some(s), Some(t)) = (sql, sql_tx) {
            let sp = speedup(s.median_us(), t.median_us());
            writeln!(
                out,
                "- **{pk}**: Wrapping SQL in a transaction gives a **{sp:.1}×** speedup over autocommit",
            )
            .unwrap();
        }
    }
    writeln!(out).unwrap();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate the full Markdown report and write it to `output_dir/report.md`.
pub fn generate_report(
    results: &ResultSet,
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut out = String::with_capacity(16 * 1024);

    write_header(&mut out);
    write_summary_table(&mut out, results);
    write_scaling_section(&mut out);
    write_method_comparison(&mut out, results);
    write_config_section(&mut out, results);
    write_pk_section(&mut out, results);
    write_generation_section(&mut out, results);
    write_key_findings(&mut out, results);

    let report_path = output_dir.join("report.md");
    std::fs::write(&report_path, &out)?;
    eprintln!("    Saved: {}", report_path.display());
    Ok(())
}
