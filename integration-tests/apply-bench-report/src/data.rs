//! Data loading module — reads Criterion benchmark results from disk.
//!
//! Walks `target/criterion/*/new/` directories, parses `benchmark.json` for
//! metadata and `estimates.json` for timing data, and exposes query helpers to
//! slice results by dimension.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

// ---------------------------------------------------------------------------
// Raw Criterion JSON shapes
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CriterionBenchmark {
    group_id: String,
    function_id: String,
}

#[derive(Deserialize)]
struct ConfidenceInterval {
    lower_bound: f64,
    upper_bound: f64,
}

#[derive(Deserialize)]
struct Estimate {
    confidence_interval: ConfidenceInterval,
    point_estimate: f64,
}

#[derive(Deserialize)]
struct Estimates {
    mean: Option<Estimate>,
    median: Option<Estimate>,
    std_dev: Option<Estimate>,
}

// ---------------------------------------------------------------------------
// Parsed benchmark result
// ---------------------------------------------------------------------------

/// A single benchmark measurement with all timing statistics (in nanoseconds).
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Criterion `group_id`, e.g. `"apply/int_pk/populated/1000"`.
    pub group_id: String,
    /// Criterion `function_id`, e.g. `"changeset"`.
    pub function_id: String,
    /// Mean execution time in nanoseconds.
    pub mean_ns: f64,
    /// Median execution time in nanoseconds.
    pub median_ns: f64,
    /// Standard deviation in nanoseconds.
    pub std_dev_ns: f64,
    /// 95% CI lower bound of the mean (ns).
    pub mean_lower_ns: f64,
    /// 95% CI upper bound of the mean (ns).
    pub mean_upper_ns: f64,
}

impl BenchmarkResult {
    /// Mean in microseconds.
    pub fn mean_us(&self) -> f64 {
        self.mean_ns / 1_000.0
    }

    /// Median in microseconds.
    pub fn median_us(&self) -> f64 {
        self.median_ns / 1_000.0
    }

    /// Standard deviation in microseconds.
    pub fn std_dev_us(&self) -> f64 {
        self.std_dev_ns / 1_000.0
    }
}

// ---------------------------------------------------------------------------
// Parsed dimensions for apply benchmarks
// ---------------------------------------------------------------------------

/// Parsed dimensions from an apply benchmark `group_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApplyDimensions {
    /// `"int_pk"` or `"uuid_pk"`.
    pub pk_kind: String,
    /// `"empty"` or `"populated"`.
    pub state: String,
    /// Number of operations: 30, 100, or 1000.
    pub op_count: usize,
    /// DB config variant: `"base"`, `"indexed"`, `"triggers"`, `"fk"`.
    pub config: String,
}

/// Try to parse apply benchmark dimensions from a `group_id` string.
///
/// Expected formats:
/// - `"apply/int_pk/empty/30"` → config = `"base"`
/// - `"apply/int_pk/populated/1000/indexed"` → config = `"indexed"`
fn parse_apply_dimensions(group_id: &str) -> Option<ApplyDimensions> {
    let parts: Vec<&str> = group_id.split('/').collect();
    if parts.first() != Some(&"apply") || parts.len() < 4 {
        return None;
    }
    let pk_kind = parts[1].to_string();
    let state = parts[2].to_string();
    let op_count: usize = parts[3].parse().ok()?;
    let config = if parts.len() >= 5 {
        parts[4].to_string()
    } else {
        "base".to_string()
    };
    Some(ApplyDimensions {
        pk_kind,
        state,
        op_count,
        config,
    })
}

// ---------------------------------------------------------------------------
// Result collection
// ---------------------------------------------------------------------------

/// All loaded benchmark results with query helpers.
pub struct ResultSet {
    pub results: Vec<BenchmarkResult>,
}

impl ResultSet {
    /// Load all Criterion results from the given directory.
    ///
    /// Walks every `<criterion_dir>/<group>/<function>/new/` looking for
    /// `benchmark.json` and `estimates.json`.
    pub fn load(criterion_dir: &Path) -> Self {
        let mut results = Vec::new();

        let Ok(groups) = std::fs::read_dir(criterion_dir) else {
            eprintln!(
                "warning: cannot read criterion directory: {}",
                criterion_dir.display()
            );
            return Self { results };
        };

        for group_entry in groups.flatten() {
            let group_path = group_entry.path();
            if !group_path.is_dir() {
                continue;
            }

            let Ok(functions) = std::fs::read_dir(&group_path) else {
                continue;
            };

            for func_entry in functions.flatten() {
                let func_path = func_entry.path();
                if !func_path.is_dir() {
                    continue;
                }

                let new_dir = func_path.join("new");
                if !new_dir.is_dir() {
                    continue;
                }

                let bench_path = new_dir.join("benchmark.json");
                let est_path = new_dir.join("estimates.json");

                if !bench_path.exists() || !est_path.exists() {
                    continue;
                }

                let Ok(bench_bytes) = std::fs::read_to_string(&bench_path) else {
                    continue;
                };
                let Ok(est_bytes) = std::fs::read_to_string(&est_path) else {
                    continue;
                };

                let Ok(bench): Result<CriterionBenchmark, _> = serde_json::from_str(&bench_bytes)
                else {
                    continue;
                };
                let Ok(est): Result<Estimates, _> = serde_json::from_str(&est_bytes) else {
                    continue;
                };

                let Some(mean) = &est.mean else { continue };
                let Some(median) = &est.median else { continue };
                let Some(std_dev) = &est.std_dev else {
                    continue;
                };

                results.push(BenchmarkResult {
                    group_id: bench.group_id,
                    function_id: bench.function_id,
                    mean_ns: mean.point_estimate,
                    median_ns: median.point_estimate,
                    std_dev_ns: std_dev.point_estimate,
                    mean_lower_ns: mean.confidence_interval.lower_bound,
                    mean_upper_ns: mean.confidence_interval.upper_bound,
                });
            }
        }

        eprintln!("Loaded {} benchmark results", results.len());
        Self { results }
    }

    // -----------------------------------------------------------------------
    // Query helpers
    // -----------------------------------------------------------------------

    /// Return only the apply benchmarks, grouped by parsed dimensions.
    pub fn apply_results(&self) -> Vec<(&BenchmarkResult, ApplyDimensions)> {
        self.results
            .iter()
            .filter_map(|r| parse_apply_dimensions(&r.group_id).map(|d| (r, d)))
            .collect()
    }

    /// Return only the generation benchmarks (changeset_generation, patchset_generation).
    pub fn generation_results(&self) -> Vec<&BenchmarkResult> {
        self.results
            .iter()
            .filter(|r| r.group_id.ends_with("_generation"))
            .collect()
    }

    /// Look up a specific apply benchmark result.
    pub fn find_apply(
        &self,
        pk_kind: &str,
        state: &str,
        op_count: usize,
        config: &str,
        method: &str,
    ) -> Option<&BenchmarkResult> {
        self.apply_results().into_iter().find_map(|(r, d)| {
            if d.pk_kind == pk_kind
                && d.state == state
                && d.op_count == op_count
                && d.config == config
                && r.function_id == method
            {
                Some(r)
            } else {
                None
            }
        })
    }

    /// Group apply results by `(pk_kind, state, config)` → op_count → method → result.
    ///
    /// Useful for building scaling charts (one line per method, x = op_count).
    pub fn scaling_groups(
        &self,
    ) -> BTreeMap<(String, String, String), BTreeMap<usize, BTreeMap<String, &BenchmarkResult>>>
    {
        let mut groups: BTreeMap<
            (String, String, String),
            BTreeMap<usize, BTreeMap<String, &BenchmarkResult>>,
        > = BTreeMap::new();

        for (r, d) in self.apply_results() {
            groups
                .entry((d.pk_kind.clone(), d.state.clone(), d.config.clone()))
                .or_default()
                .entry(d.op_count)
                .or_default()
                .insert(r.function_id.clone(), r);
        }
        groups
    }

    /// Group apply results by `(pk_kind, op_count, method)` → config → result.
    ///
    /// Useful for config variant comparison charts.
    pub fn config_groups(
        &self,
    ) -> BTreeMap<(String, usize, String), BTreeMap<String, &BenchmarkResult>> {
        let mut groups: BTreeMap<(String, usize, String), BTreeMap<String, &BenchmarkResult>> =
            BTreeMap::new();

        for (r, d) in self.apply_results() {
            if d.state == "populated" {
                groups
                    .entry((d.pk_kind.clone(), d.op_count, r.function_id.clone()))
                    .or_default()
                    .insert(d.config.clone(), r);
            }
        }
        groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_base() {
        let d = parse_apply_dimensions("apply/int_pk/empty/30").unwrap();
        assert_eq!(d.pk_kind, "int_pk");
        assert_eq!(d.state, "empty");
        assert_eq!(d.op_count, 30);
        assert_eq!(d.config, "base");
    }

    #[test]
    fn test_parse_variant() {
        let d = parse_apply_dimensions("apply/uuid_pk/populated/1000/indexed").unwrap();
        assert_eq!(d.pk_kind, "uuid_pk");
        assert_eq!(d.state, "populated");
        assert_eq!(d.op_count, 1000);
        assert_eq!(d.config, "indexed");
    }

    #[test]
    fn test_parse_non_apply() {
        assert!(parse_apply_dimensions("changeset_generation").is_none());
    }

    #[test]
    fn test_parse_too_short() {
        assert!(parse_apply_dimensions("apply/int_pk/empty").is_none());
    }

    #[test]
    fn test_parse_invalid_op_count() {
        assert!(parse_apply_dimensions("apply/int_pk/empty/notanumber").is_none());
    }

    #[test]
    fn test_benchmark_result_conversions() {
        let result = BenchmarkResult {
            group_id: "apply/int_pk/empty/100".to_string(),
            function_id: "changeset".to_string(),
            mean_ns: 5_000_000.0,   // 5ms = 5000µs
            median_ns: 4_500_000.0, // 4.5ms = 4500µs
            std_dev_ns: 100_000.0,  // 100µs
            mean_lower_ns: 4_800_000.0,
            mean_upper_ns: 5_200_000.0,
        };

        assert!((result.mean_us() - 5000.0).abs() < 0.01);
        assert!((result.median_us() - 4500.0).abs() < 0.01);
        assert!((result.std_dev_us() - 100.0).abs() < 0.01);
    }

    fn create_test_results() -> ResultSet {
        let results = vec![
            BenchmarkResult {
                group_id: "apply/int_pk/empty/30".to_string(),
                function_id: "sql".to_string(),
                mean_ns: 1000.0,
                median_ns: 900.0,
                std_dev_ns: 50.0,
                mean_lower_ns: 950.0,
                mean_upper_ns: 1050.0,
            },
            BenchmarkResult {
                group_id: "apply/int_pk/empty/30".to_string(),
                function_id: "changeset".to_string(),
                mean_ns: 500.0,
                median_ns: 450.0,
                std_dev_ns: 25.0,
                mean_lower_ns: 475.0,
                mean_upper_ns: 525.0,
            },
            BenchmarkResult {
                group_id: "apply/int_pk/populated/1000".to_string(),
                function_id: "sql".to_string(),
                mean_ns: 10000.0,
                median_ns: 9000.0,
                std_dev_ns: 500.0,
                mean_lower_ns: 9500.0,
                mean_upper_ns: 10500.0,
            },
            BenchmarkResult {
                group_id: "apply/int_pk/populated/1000/indexed".to_string(),
                function_id: "sql".to_string(),
                mean_ns: 12000.0,
                median_ns: 11000.0,
                std_dev_ns: 600.0,
                mean_lower_ns: 11500.0,
                mean_upper_ns: 12500.0,
            },
            BenchmarkResult {
                group_id: "changeset_generation".to_string(),
                function_id: "builder".to_string(),
                mean_ns: 2000.0,
                median_ns: 1800.0,
                std_dev_ns: 100.0,
                mean_lower_ns: 1900.0,
                mean_upper_ns: 2100.0,
            },
        ];
        ResultSet { results }
    }

    #[test]
    fn test_apply_results() {
        let rs = create_test_results();
        let apply = rs.apply_results();
        assert_eq!(apply.len(), 4);
    }

    #[test]
    fn test_generation_results() {
        let rs = create_test_results();
        let r#gen = rs.generation_results();
        assert_eq!(r#gen.len(), 1);
        assert_eq!(r#gen[0].group_id, "changeset_generation");
    }

    #[test]
    fn test_find_apply() {
        let rs = create_test_results();

        let found = rs.find_apply("int_pk", "empty", 30, "base", "sql");
        assert!(found.is_some());
        assert_eq!(found.unwrap().function_id, "sql");

        let found = rs.find_apply("int_pk", "populated", 1000, "indexed", "sql");
        assert!(found.is_some());

        let not_found = rs.find_apply("uuid_pk", "empty", 30, "base", "sql");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_scaling_groups() {
        let rs = create_test_results();
        let groups = rs.scaling_groups();

        // Should have entries for (int_pk, empty, base), (int_pk, populated, base), (int_pk, populated, indexed)
        assert!(!groups.is_empty());

        let key = (
            "int_pk".to_string(),
            "empty".to_string(),
            "base".to_string(),
        );
        assert!(groups.contains_key(&key));

        let ops_map = &groups[&key];
        assert!(ops_map.contains_key(&30));
    }

    #[test]
    fn test_config_groups() {
        let rs = create_test_results();
        let groups = rs.config_groups();

        // Only populated results are included in config_groups
        // Should have entry for (int_pk, 1000, sql)
        let key = ("int_pk".to_string(), 1000, "sql".to_string());
        assert!(groups.contains_key(&key));

        let config_map = &groups[&key];
        assert!(config_map.contains_key("base"));
        assert!(config_map.contains_key("indexed"));
    }

    #[test]
    fn test_load_nonexistent_dir() {
        let rs = ResultSet::load(std::path::Path::new("/nonexistent/path"));
        assert!(rs.results.is_empty());
    }
}
