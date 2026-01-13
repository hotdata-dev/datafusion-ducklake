use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Benchmark {
    pub name: String,
    pub group: Option<String>,
    pub subgroup: Option<String>,
    pub description: Option<String>,
    pub load: Option<String>,
    pub run: String,
    pub result: Option<String>,
    pub arguments: HashMap<String, Vec<String>>,
}

pub fn parse_benchmark_file(path: &Path) -> Result<Vec<Benchmark>> {
    let content = fs::read_to_string(path)?;
    parse_benchmark(&content, path.to_string_lossy().to_string())
}

fn parse_benchmark(content: &str, default_name: String) -> Result<Vec<Benchmark>> {
    let mut name = default_name;
    let mut group = None;
    let mut subgroup = None;
    let mut description = None;
    let mut load = None;
    let mut run = None;
    let mut result = None;
    let mut arguments: HashMap<String, Vec<String>> = HashMap::new();

    let mut current_section: Option<&str> = None;
    let mut section_content = String::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Parse header comments
        if trimmed.starts_with("# name:") {
            name = trimmed.strip_prefix("# name:").unwrap().trim().to_string();
            continue;
        }
        if trimmed.starts_with("# group:") {
            group = Some(trimmed.strip_prefix("# group:").unwrap().trim().to_string());
            continue;
        }
        if trimmed.starts_with("# subgroup:") {
            subgroup = Some(trimmed.strip_prefix("# subgroup:").unwrap().trim().to_string());
            continue;
        }
        if trimmed.starts_with("# description:") {
            description = Some(trimmed.strip_prefix("# description:").unwrap().trim().to_string());
            continue;
        }
        if trimmed.starts_with("# argument:") {
            let arg = trimmed.strip_prefix("# argument:").unwrap().trim();
            if let Some((key, values)) = arg.split_once('=') {
                let vals: Vec<String> = values.split(',').map(|s| s.trim().to_string()).collect();
                arguments.insert(key.trim().to_string(), vals);
            }
            continue;
        }

        // Skip other comments
        if trimmed.starts_with('#') {
            continue;
        }

        // Section markers
        if trimmed == "load" {
            save_section(&current_section, &section_content, &mut load, &mut run, &mut result);
            current_section = Some("load");
            section_content.clear();
            continue;
        }
        if trimmed == "run" {
            save_section(&current_section, &section_content, &mut load, &mut run, &mut result);
            current_section = Some("run");
            section_content.clear();
            continue;
        }
        if trimmed == "result" {
            save_section(&current_section, &section_content, &mut load, &mut run, &mut result);
            current_section = Some("result");
            section_content.clear();
            continue;
        }

        // Accumulate section content
        if current_section.is_some() {
            if !section_content.is_empty() {
                section_content.push('\n');
            }
            section_content.push_str(line);
        }
    }

    // Save final section
    save_section(&current_section, &section_content, &mut load, &mut run, &mut result);

    let run_sql = run.ok_or_else(|| anyhow!("No 'run' section found in benchmark"))?;

    // Expand arguments into multiple benchmarks
    let benchmarks = expand_arguments(Benchmark {
        name,
        group,
        subgroup,
        description,
        load,
        run: run_sql,
        result,
        arguments,
    });

    Ok(benchmarks)
}

fn save_section(
    current: &Option<&str>,
    content: &str,
    load: &mut Option<String>,
    run: &mut Option<String>,
    result: &mut Option<String>,
) {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return;
    }

    match current {
        Some("load") => *load = Some(trimmed.to_string()),
        Some("run") => *run = Some(trimmed.to_string()),
        Some("result") => *result = Some(trimmed.to_string()),
        _ => {}
    }
}

fn expand_arguments(base: Benchmark) -> Vec<Benchmark> {
    if base.arguments.is_empty() {
        return vec![base];
    }

    // Get all argument combinations
    let mut combinations: Vec<HashMap<String, String>> = vec![HashMap::new()];

    for (key, values) in &base.arguments {
        let mut new_combinations = Vec::new();
        for combo in &combinations {
            for value in values {
                let mut new_combo = combo.clone();
                new_combo.insert(key.clone(), value.clone());
                new_combinations.push(new_combo);
            }
        }
        combinations = new_combinations;
    }

    // Generate benchmarks for each combination
    combinations
        .into_iter()
        .map(|combo| {
            let mut benchmark = base.clone();

            // Update name with argument values
            let suffix: String = combo.values().cloned().collect::<Vec<_>>().join("_");
            if !suffix.is_empty() {
                benchmark.name = format!("{}_{}", benchmark.name, suffix);
            }

            // Substitute variables in SQL
            benchmark.run = substitute_vars(&benchmark.run, &combo);
            if let Some(ref load) = benchmark.load {
                benchmark.load = Some(substitute_vars(load, &combo));
            }

            benchmark
        })
        .collect()
}

fn substitute_vars(sql: &str, vars: &HashMap<String, String>) -> String {
    let mut result = sql.to_string();
    for (key, value) in vars {
        result = result.replace(&format!("${{{}}}", key), value);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_benchmark() {
        let content = r#"
# name: test/sum.benchmark
# group: micro
# description: Sum benchmark

load
CREATE TABLE t AS SELECT range i FROM range(1000);

run
SELECT SUM(i) FROM t;

result
499500
"#;

        let benchmarks = parse_benchmark(content, "default".to_string()).unwrap();
        assert_eq!(benchmarks.len(), 1);
        assert_eq!(benchmarks[0].name, "test/sum.benchmark");
        assert_eq!(benchmarks[0].group, Some("micro".to_string()));
        assert!(benchmarks[0].load.is_some());
        assert_eq!(benchmarks[0].run, "SELECT SUM(i) FROM t;");
    }
}
