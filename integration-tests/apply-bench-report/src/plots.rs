//! SVG chart generation for apply-benchmark results.
//!
//! Produces line charts (scaling) and grouped-bar charts (comparisons) using
//! the `plotters` crate with the SVG backend.  Follows the palette and styling
//! conventions from `integration-tests/payload-size-bench/src/plots.rs`.

use plotters::prelude::*;
use std::path::Path;

use crate::data::{BenchmarkResult, ResultSet};

// ---------------------------------------------------------------------------
// Palette — 4 distinguishable colors for the four apply methods
// ---------------------------------------------------------------------------

const COLOR_SQL: RGBColor = RGBColor(231, 76, 60); //  red
const COLOR_SQL_TX: RGBColor = RGBColor(230, 160, 0); //  amber
const COLOR_PATCHSET: RGBColor = RGBColor(52, 152, 219); //  blue
const COLOR_CHANGESET: RGBColor = RGBColor(46, 204, 113); //  emerald

/// The four apply methods in canonical display order.
pub const METHODS: &[&str] = &["sql", "sql_tx", "patchset", "changeset"];

/// Method display names (for legends and tables).
pub const METHOD_LABELS: &[&str] = &[
    "SQL (autocommit)",
    "SQL (transaction)",
    "Patchset",
    "Changeset",
];

fn method_color(method: &str) -> RGBColor {
    match method {
        "sql" => COLOR_SQL,
        "sql_tx" => COLOR_SQL_TX,
        "patchset" => COLOR_PATCHSET,
        "changeset" => COLOR_CHANGESET,
        _ => RGBColor(128, 128, 128),
    }
}

fn method_label(method: &str) -> &str {
    match method {
        "sql" => "SQL (autocommit)",
        "sql_tx" => "SQL (transaction)",
        "patchset" => "Patchset",
        "changeset" => "Changeset",
        _ => "unknown",
    }
}

fn method_stroke(method: &str) -> u32 {
    match method {
        "patchset" | "changeset" => 3,
        _ => 2,
    }
}

// ---------------------------------------------------------------------------
// Y-axis label formatter (human-readable µs / ms suffixes)
// ---------------------------------------------------------------------------

fn y_fmt(y: &f64) -> String {
    let v = *y;
    if v >= 1_000.0 {
        format!("{:.1} ms", v / 1_000.0)
    } else if v >= 1.0 {
        format!("{:.0} µs", v)
    } else {
        format!("{:.1} µs", v)
    }
}

// ---------------------------------------------------------------------------
// Chart A — Scaling by method (line chart, log-ish x-axis)
// ---------------------------------------------------------------------------

/// Draw a scaling line chart for a single `(pk_kind, state, config)` group.
///
/// X-axis: operation count (30, 100, 1000).  Y-axis: median time (µs).
/// One line per apply method with error bars from the confidence interval.
///
/// Returns `Ok(())` on success or an error if chart generation fails.
pub fn scaling_chart(
    results: &ResultSet,
    pk_kind: &str,
    state: &str,
    config: &str,
    output: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let op_counts: &[usize] = &[30, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];

    // Collect data points per method.
    let mut series: Vec<(&str, Vec<(f64, f64, f64, f64)>)> = Vec::new();
    for &method in METHODS {
        let mut points = Vec::new();
        for &ops in op_counts {
            if let Some(r) = results.find_apply(pk_kind, state, ops, config, method) {
                let x = ops as f64;
                let y = r.median_us();
                let lo = r.mean_lower_ns / 1_000.0;
                let hi = r.mean_upper_ns / 1_000.0;
                points.push((x, y, lo, hi));
            }
        }
        if !points.is_empty() {
            series.push((method, points));
        }
    }

    if series.is_empty() {
        return Ok(());
    }

    let y_max = series
        .iter()
        .flat_map(|(_, pts)| pts.iter().map(|(_, _, _, hi)| *hi))
        .fold(0.0f64, f64::max);

    let title = format!(
        "Apply Scaling — {}, {}, {}",
        pk_kind.replace('_', " "),
        state,
        config,
    );

    let root = SVGBackend::new(output, (900, 540)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption(&title, ("sans-serif", 18))
        .margin(14)
        .x_label_area_size(40)
        .y_label_area_size(80)
        .build_cartesian_2d((30f64..1100f64).log_scale(), 0f64..y_max * 1.15)?;

    chart
        .configure_mesh()
        .x_desc("Number of Operations")
        .y_desc("Median Time (µs)")
        .y_label_formatter(&y_fmt)
        .x_label_formatter(&|x| format!("{}", *x as usize))
        .draw()?;

    for (method, points) in &series {
        let color = method_color(method);
        let width = method_stroke(method);
        let label = method_label(method);

        // Error bars.
        let x_max_val = 1100f64;
        for &(x, _y, lo, hi) in points {
            let cap_w = x_max_val * 0.008;
            chart.draw_series(std::iter::once(PathElement::new(
                vec![(x, lo), (x, hi)],
                color.mix(0.5).stroke_width(1),
            )))?;
            chart.draw_series(std::iter::once(PathElement::new(
                vec![(x - cap_w, lo), (x + cap_w, lo)],
                color.mix(0.5).stroke_width(1),
            )))?;
            chart.draw_series(std::iter::once(PathElement::new(
                vec![(x - cap_w, hi), (x + cap_w, hi)],
                color.mix(0.5).stroke_width(1),
            )))?;
        }

        // Line + dots.
        let xy: Vec<(f64, f64)> = points.iter().map(|&(x, y, _, _)| (x, y)).collect();
        chart
            .draw_series(LineSeries::new(xy.clone(), color.stroke_width(width)))?
            .label(label)
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(width))
            });
        chart.draw_series(
            xy.iter()
                .map(|&(x, y)| Circle::new((x, y), 3, color.filled())),
        )?;
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .margin(12)
        .background_style(WHITE.mix(0.9))
        .border_style(BLACK.mix(0.3))
        .label_font(("sans-serif", 13))
        .draw()?;

    root.present()?;
    eprintln!("    Saved: {}", output.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Chart B — Method comparison (grouped bar chart)
// ---------------------------------------------------------------------------

/// Draw a grouped bar chart comparing methods for a specific scenario.
///
/// One cluster of 4 bars (one per method) showing median time with error
/// whiskers from the confidence interval.
pub fn method_comparison_chart(
    results: &ResultSet,
    pk_kind: &str,
    state: &str,
    op_count: usize,
    config: &str,
    output: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut data: Vec<(&str, f64, f64, f64)> = Vec::new(); // (method, median, lo, hi)
    for &method in METHODS {
        if let Some(r) = results.find_apply(pk_kind, state, op_count, config, method) {
            data.push((
                method,
                r.median_us(),
                r.mean_lower_ns / 1_000.0,
                r.mean_upper_ns / 1_000.0,
            ));
        }
    }

    if data.is_empty() {
        return Ok(());
    }

    let y_max = data.iter().map(|(_, _, _, hi)| *hi).fold(0.0f64, f64::max);

    let title = format!(
        "Method Comparison — {} {}/{} ops ({})",
        pk_kind.replace('_', " "),
        state,
        op_count,
        config,
    );

    let root = SVGBackend::new(output, (700, 480)).into_drawing_area();
    root.fill(&WHITE)?;

    let n = data.len();
    let mut chart = ChartBuilder::on(&root)
        .caption(&title, ("sans-serif", 18))
        .margin(14)
        .x_label_area_size(50)
        .y_label_area_size(80)
        .build_cartesian_2d(0f64..n as f64, 0f64..y_max * 1.2)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(n * 2 + 1)
        .y_desc("Median Time (µs)")
        .y_label_formatter(&y_fmt)
        .x_label_formatter(&|x| {
            // Only label at bar centers (0.5, 1.5, …).
            let centered = x - 0.5;
            if (centered - centered.round()).abs() > 0.1 || centered < 0.0 {
                return String::new();
            }
            let idx = centered.round() as usize;
            data.get(idx)
                .map(|(m, _, _, _)| method_label(m).to_string())
                .unwrap_or_default()
        })
        .draw()?;

    // Draw bars.
    for (i, (method, median, lo, hi)) in data.iter().enumerate() {
        let color = method_color(method);
        let x0 = i as f64;
        let x1 = x0 + 1.0;
        chart.draw_series(std::iter::once(Rectangle::new(
            [(x0, 0.0), (x1, *median)],
            color.mix(0.8).filled(),
        )))?;
        // Error whiskers centered in the bar.
        let cx = x0 + 0.5;
        chart.draw_series(std::iter::once(PathElement::new(
            vec![(cx, *lo), (cx, *hi)],
            BLACK.stroke_width(1),
        )))?;
    }

    root.present()?;
    eprintln!("    Saved: {}", output.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Chart C — Config variant impact (grouped line chart)
// ---------------------------------------------------------------------------

/// Draw a chart comparing config variants for `populated/1000`.
///
/// X-axis: config variants (base, indexed, triggers, fk).
/// One line per method.
pub fn config_variant_chart(
    results: &ResultSet,
    pk_kind: &str,
    output: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let configs = ["base", "indexed", "triggers", "fk"];
    let op_count = 1000;
    let state = "populated";

    let mut series: Vec<(&str, Vec<(f64, f64, f64, f64)>)> = Vec::new();
    for &method in METHODS {
        let mut points = Vec::new();
        for (ci, &cfg) in configs.iter().enumerate() {
            if let Some(r) = results.find_apply(pk_kind, state, op_count, cfg, method) {
                let x = ci as f64;
                points.push((
                    x,
                    r.median_us(),
                    r.mean_lower_ns / 1_000.0,
                    r.mean_upper_ns / 1_000.0,
                ));
            }
        }
        if !points.is_empty() {
            series.push((method, points));
        }
    }

    if series.is_empty() {
        return Ok(());
    }

    let y_max = series
        .iter()
        .flat_map(|(_, pts)| pts.iter().map(|(_, _, _, hi)| *hi))
        .fold(0.0f64, f64::max);

    let title = format!(
        "Config Variant Impact — {} populated/1000",
        pk_kind.replace('_', " "),
    );

    let root = SVGBackend::new(output, (800, 520)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption(&title, ("sans-serif", 18))
        .margin(14)
        .x_label_area_size(44)
        .y_label_area_size(80)
        .build_cartesian_2d(-0.5f64..3.5f64, 0f64..y_max * 1.15)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(9)
        .x_desc("DB Configuration")
        .y_desc("Median Time (µs)")
        .y_label_formatter(&y_fmt)
        .x_label_formatter(&|x| {
            let idx = x.round() as usize;
            // Only label exact integer positions to avoid duplicates.
            if (*x - x.round()).abs() > 0.1 {
                return String::new();
            }
            configs.get(idx).unwrap_or(&"").to_string()
        })
        .draw()?;

    for (method, points) in &series {
        let color = method_color(method);
        let width = method_stroke(method);
        let label = method_label(method);

        // Error bars.
        for &(x, _y, lo, hi) in points {
            let cap_w = 0.06;
            chart.draw_series(std::iter::once(PathElement::new(
                vec![(x, lo), (x, hi)],
                color.mix(0.5).stroke_width(1),
            )))?;
            chart.draw_series(std::iter::once(PathElement::new(
                vec![(x - cap_w, lo), (x + cap_w, lo)],
                color.mix(0.5).stroke_width(1),
            )))?;
            chart.draw_series(std::iter::once(PathElement::new(
                vec![(x - cap_w, hi), (x + cap_w, hi)],
                color.mix(0.5).stroke_width(1),
            )))?;
        }

        let xy: Vec<(f64, f64)> = points.iter().map(|&(x, y, _, _)| (x, y)).collect();
        chart
            .draw_series(LineSeries::new(xy.clone(), color.stroke_width(width)))?
            .label(label)
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(width))
            });
        chart.draw_series(
            xy.iter()
                .map(|&(x, y)| Circle::new((x, y), 4, color.filled())),
        )?;
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .margin(12)
        .background_style(WHITE.mix(0.9))
        .border_style(BLACK.mix(0.3))
        .label_font(("sans-serif", 13))
        .draw()?;

    root.present()?;
    eprintln!("    Saved: {}", output.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Chart D — PK type comparison (grouped bars)
// ---------------------------------------------------------------------------

/// Draw a grouped bar chart comparing int_pk vs uuid_pk for `populated/1000/base`.
pub fn pk_comparison_chart(
    results: &ResultSet,
    output: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = "populated";
    let op_count = 1000;
    let config = "base";
    let pks = ["int_pk", "uuid_pk"];

    // Collect: method → [int_pk median, uuid_pk median].
    let mut y_max: f64 = 0.0;
    let mut data: Vec<(&str, [Option<&BenchmarkResult>; 2])> = Vec::new();
    for &method in METHODS {
        let mut pair: [Option<&BenchmarkResult>; 2] = [None, None];
        for (pi, &pk) in pks.iter().enumerate() {
            if let Some(r) = results.find_apply(pk, state, op_count, config, method) {
                if r.mean_upper_ns / 1_000.0 > y_max {
                    y_max = r.mean_upper_ns / 1_000.0;
                }
                pair[pi] = Some(r);
            }
        }
        data.push((method, pair));
    }

    if data.is_empty() {
        return Ok(());
    }

    let title = "PK Type Comparison — populated/1000 (base)";

    let root = SVGBackend::new(output, (800, 480)).into_drawing_area();
    root.fill(&WHITE)?;

    // X range: 4 method groups × 2 bars each = 8 slots.
    let total_bars = METHODS.len() * 2;
    let mut chart = ChartBuilder::on(&root)
        .caption(title, ("sans-serif", 18))
        .margin(14)
        .x_label_area_size(60)
        .y_label_area_size(80)
        .build_cartesian_2d(0f64..total_bars as f64, 0f64..y_max * 1.2)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(total_bars + 1)
        .y_desc("Median Time (µs)")
        .y_label_formatter(&y_fmt)
        .x_label_formatter(&|x| {
            // Label at the center of each pair (0.5+1=1.0, not quite — center of pair i is i*2 + 1).
            // Each method group spans 2 bars: [mi*2, mi*2+2). Center = mi*2 + 1.
            let center_offset = *x;
            // Check if x is an odd integer (center of a pair).
            if (center_offset - center_offset.round()).abs() > 0.1 {
                return String::new();
            }
            let ix = center_offset.round() as usize;
            if ix % 2 == 1 {
                let method_idx = ix / 2;
                data.get(method_idx)
                    .map(|(m, _)| method_label(m).to_string())
                    .unwrap_or_default()
            } else {
                String::new()
            }
        })
        .draw()?;

    let pk_colors = [RGBColor(52, 152, 219), RGBColor(230, 160, 0)]; // blue = int, amber = uuid

    for (mi, (_method, pair)) in data.iter().enumerate() {
        for (pi, maybe_r) in pair.iter().enumerate() {
            if let Some(r) = maybe_r {
                let bar_idx = mi * 2 + pi;
                let color = pk_colors[pi];
                let median = r.median_us();
                let x0 = bar_idx as f64;
                let x1 = x0 + 1.0;
                chart.draw_series(std::iter::once(Rectangle::new(
                    [(x0, 0.0), (x1, median)],
                    color.mix(0.8).filled(),
                )))?;

                // Error whisker centered in bar.
                let cx = x0 + 0.5;
                let lo = r.mean_lower_ns / 1_000.0;
                let hi = r.mean_upper_ns / 1_000.0;
                chart.draw_series(std::iter::once(PathElement::new(
                    vec![(cx, lo), (cx, hi)],
                    BLACK.stroke_width(1),
                )))?;
            }
        }
    }

    // Manual legend.
    let legend_items = [("INTEGER PK", pk_colors[0]), ("UUID BLOB PK", pk_colors[1])];
    for (li, (label, color)) in legend_items.iter().enumerate() {
        let x_px = 680;
        let y_px = 60 + li as i32 * 22;
        root.draw(&Rectangle::new(
            [(x_px, y_px), (x_px + 16, y_px + 12)],
            color.filled(),
        ))?;
        root.draw(&Text::new(
            *label,
            (x_px + 22, y_px),
            ("sans-serif", 13).into_font(),
        ))?;
    }

    root.present()?;
    eprintln!("    Saved: {}", output.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Generate all charts
// ---------------------------------------------------------------------------

/// Generate all SVG charts into `output_dir`.
pub fn generate_all(
    results: &ResultSet,
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(output_dir)?;

    // Chart A — Scaling charts: one per (pk_kind, state) with config=base.
    for pk in &["int_pk", "uuid_pk"] {
        for state in &["empty", "populated"] {
            let filename = format!("scaling_{pk}_{state}.svg");
            scaling_chart(results, pk, state, "base", &output_dir.join(&filename))?;
        }
    }

    // Chart B — Method comparison for populated/1000/base.
    for pk in &["int_pk", "uuid_pk"] {
        let filename = format!("method_{pk}.svg");
        method_comparison_chart(
            results,
            pk,
            "populated",
            1000,
            "base",
            &output_dir.join(&filename),
        )?;
    }

    // Chart C — Config variant impact.
    for pk in &["int_pk", "uuid_pk"] {
        let filename = format!("config_{pk}.svg");
        config_variant_chart(results, pk, &output_dir.join(&filename))?;
    }

    // Chart D — PK type comparison.
    pk_comparison_chart(results, &output_dir.join("pk_comparison.svg"))?;

    Ok(())
}
