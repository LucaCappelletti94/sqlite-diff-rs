//! SVG plot generation for payload size comparisons.
//!
//! Produces a single linear-scale SVG chart using `plotters` with the SVG
//! backend.

use plotters::prelude::*;
use std::path::Path;

// ---------------------------------------------------------------------------
// Palette — 7 distinguishable colors (loosely based on ColorBrewer Set1)
// ---------------------------------------------------------------------------

const PALETTE: [RGBColor; 7] = [
    RGBColor(231, 76, 60),  // SQL        — red
    RGBColor(230, 160, 0),  // JSON       — amber
    RGBColor(46, 204, 113), // MsgPack    — emerald
    RGBColor(26, 188, 156), // CBOR       — turquoise
    RGBColor(52, 152, 219), // Protobuf   — blue
    RGBColor(44, 62, 80),   // Patchset   — dark slate
    RGBColor(155, 89, 182), // Changeset  — amethyst
];

/// Stroke widths — the two "winners" (Protobuf, Patchset) are drawn thicker.
const STROKE_W: [u32; 7] = [2, 2, 2, 2, 3, 3, 2];

// ---------------------------------------------------------------------------
// Y-axis label formatter (human-readable K / M suffixes)
// ---------------------------------------------------------------------------

fn y_fmt(y: &f64) -> String {
    let v = *y;
    if v >= 1_000_000.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else if v >= 1_000.0 {
        format!("{:.0}K", v / 1_000.0)
    } else if v >= 1.0 {
        format!("{:.0}", v)
    } else {
        format!("{:.1}", v)
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Draw a linear-scale chart and save it as an SVG file.
///
/// * `data`    — slice of `(x, [y₀, y₁, … y₆])` data points, one per x tick.
/// * `names`   — human-readable series names (same order as the y-values)
/// * `x_label` — x-axis label
/// * `y_label` — y-axis label
/// * `output`  — destination `.svg` path
pub fn line_chart(
    data: &[(f64, Vec<f64>)],
    names: &[&str],
    x_label: &str,
    y_label: &str,
    output: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if data.is_empty() || names.is_empty() {
        return Ok(());
    }

    let x_max = data.iter().map(|(x, _)| *x).fold(0.0f64, f64::max);
    let y_max = data
        .iter()
        .flat_map(|(_, ys)| ys.iter())
        .copied()
        .fold(0.0f64, f64::max);

    let root = SVGBackend::new(output, (900, 540)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Payload Size vs Operation Count — mixed workload (60% INSERT, 25% UPDATE, 15% DELETE)",
            ("sans-serif", 17),
        )
        .margin(14)
        .x_label_area_size(40)
        .y_label_area_size(75)
        .build_cartesian_2d(0f64..x_max * 1.05, 0f64..y_max * 1.12)?;

    chart
        .configure_mesh()
        .x_desc(x_label)
        .y_desc(y_label)
        .y_label_formatter(&y_fmt)
        .draw()?;

    let n = names.len().min(PALETTE.len());
    for i in 0..n {
        let color = PALETTE[i];
        let width = STROKE_W[i];

        let points: Vec<(f64, f64)> = data
            .iter()
            .filter_map(|(x, ys)| ys.get(i).map(|y| (*x, *y)))
            .collect();

        chart
            .draw_series(LineSeries::new(points.clone(), color.stroke_width(width)))?
            .label(names[i])
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(width))
            });

        chart.draw_series(
            points
                .iter()
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
    println!("    📊 Saved: {}", output.display());
    Ok(())
}

/// Draw a stacked chart with size (top) and time with error bars (bottom).
///
/// * `size_data`   — slice of `(x, [y₀, y₁, … y₆])` for compressed sizes.
/// * `time_data`   — slice of `(x, [y₀, y₁, …], [err₀, err₁, …])` for timing.
/// * `names`       — human-readable series names (same order as y-values)
/// * `x_label`     — x-axis label
/// * `title`       — chart title (e.g. "DEFLATE Compression")
/// * `output`      — destination `.svg` path
pub fn stacked_size_and_time(
    size_data: &[(f64, Vec<f64>)],
    time_data: &[(f64, Vec<f64>, Vec<f64>)],
    names: &[&str],
    x_label: &str,
    title: &str,
    output: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if size_data.is_empty() || names.is_empty() {
        return Ok(());
    }

    let x_max = size_data.iter().map(|(x, _)| *x).fold(0.0f64, f64::max);
    let size_y_max = size_data
        .iter()
        .flat_map(|(_, ys)| ys.iter())
        .copied()
        .fold(0.0f64, f64::max);
    let time_y_max = time_data
        .iter()
        .flat_map(|(_, ys, errs)| ys.iter().zip(errs.iter()).map(|(y, e)| y + e))
        .fold(0.0f64, f64::max);

    let root = SVGBackend::new(output, (900, 860)).into_drawing_area();
    root.fill(&WHITE)?;

    // Split: title area (50px) then two chart areas
    let (title_area, charts_area) = root.split_vertically(50);
    let (upper, lower) = charts_area.split_vertically(390);

    // Draw title centered
    title_area.titled(title, ("sans-serif", 24))?;

    // =========================================================================
    // Top chart: Compressed sizes
    // =========================================================================
    let mut size_chart = ChartBuilder::on(&upper)
        .margin(16)
        .margin_top(8)
        .x_label_area_size(40)
        .y_label_area_size(80)
        .build_cartesian_2d(0f64..x_max * 1.05, 0f64..size_y_max * 1.12)?;

    size_chart
        .configure_mesh()
        .x_desc(x_label)
        .y_desc("Compressed Size (bytes)")
        .label_style(("sans-serif", 14))
        .axis_desc_style(("sans-serif", 15))
        .y_label_formatter(&y_fmt)
        .draw()?;

    let n = names.len().min(PALETTE.len());
    for i in 0..n {
        let color = PALETTE[i];
        let width = STROKE_W[i];

        let points: Vec<(f64, f64)> = size_data
            .iter()
            .filter_map(|(x, ys)| ys.get(i).map(|y| (*x, *y)))
            .collect();

        size_chart
            .draw_series(LineSeries::new(points.clone(), color.stroke_width(width)))?
            .label(names[i])
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(width))
            });

        size_chart.draw_series(
            points
                .iter()
                .map(|&(x, y)| Circle::new((x, y), 3, color.filled())),
        )?;
    }

    size_chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .margin(12)
        .background_style(WHITE.mix(0.9))
        .border_style(BLACK.mix(0.3))
        .label_font(("sans-serif", 14))
        .draw()?;

    // =========================================================================
    // Bottom chart: Timing with error bars
    // =========================================================================
    let mut time_chart = ChartBuilder::on(&lower)
        .margin(16)
        .margin_top(12)
        .x_label_area_size(40)
        .y_label_area_size(80)
        .build_cartesian_2d(0f64..x_max * 1.05, 0f64..time_y_max * 1.12)?;

    time_chart
        .configure_mesh()
        .x_desc(x_label)
        .y_desc("Compress + Decompress Time (µs, ±1σ)")
        .label_style(("sans-serif", 14))
        .axis_desc_style(("sans-serif", 15))
        .y_label_formatter(&y_fmt)
        .draw()?;

    for i in 0..n {
        let color = PALETTE[i];
        let width = STROKE_W[i];

        let points: Vec<(f64, f64)> = time_data
            .iter()
            .filter_map(|(x, ys, _)| ys.get(i).map(|y| (*x, *y)))
            .collect();

        let error_bars: Vec<(f64, f64, f64)> = time_data
            .iter()
            .filter_map(|(x, ys, errs)| ys.get(i).and_then(|y| errs.get(i).map(|e| (*x, *y, *e))))
            .collect();

        // Draw error bars first (behind the line)
        for (x, y, err) in &error_bars {
            let y_lo = (y - err).max(0.0);
            let y_hi = y + err;
            // Vertical line
            time_chart.draw_series(std::iter::once(PathElement::new(
                vec![(*x, y_lo), (*x, y_hi)],
                color.mix(0.5).stroke_width(1),
            )))?;
            // Horizontal caps
            let cap_w = x_max * 0.008;
            time_chart.draw_series(std::iter::once(PathElement::new(
                vec![(x - cap_w, y_lo), (x + cap_w, y_lo)],
                color.mix(0.5).stroke_width(1),
            )))?;
            time_chart.draw_series(std::iter::once(PathElement::new(
                vec![(x - cap_w, y_hi), (x + cap_w, y_hi)],
                color.mix(0.5).stroke_width(1),
            )))?;
        }

        time_chart.draw_series(LineSeries::new(points.clone(), color.stroke_width(width)))?;

        time_chart.draw_series(
            points
                .iter()
                .map(|&(x, y)| Circle::new((x, y), 3, color.filled())),
        )?;
    }

    root.present()?;
    println!("    📊 Saved: {}", output.display());
    Ok(())
}
