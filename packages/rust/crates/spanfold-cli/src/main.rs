#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Spanfold command-line entry point.

use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use spanfold::{
    AgainstSelection, Comparator, ComparisonFinality, ComparisonPlan, ContractFixture,
    OpenWindowPolicy, PrimitiveValue, WindowHistoryFixture, compare, export_result_debug_html,
    export_result_json, export_result_llm_context, export_result_markdown,
};
use std::{fs, process::ExitCode};

/// Production high-throughput CLI for Spanfold temporal evidence workflows.
#[derive(Debug, Parser)]
#[command(name = "spanfold")]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Spanfold CLI commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Validate a Spanfold fixture plan.
    ValidatePlan {
        /// Fixture JSON path.
        fixture: String,
    },
    /// Compare a Spanfold fixture.
    Compare {
        /// Fixture JSON path.
        fixture: String,
        /// Output format.
        #[arg(long, default_value = "json")]
        format: OutputFormat,
    },
    /// Explain a Spanfold fixture as Markdown.
    Explain {
        /// Fixture JSON path.
        fixture: String,
    },
    /// Write a full audit artifact bundle from a fixture.
    Audit {
        /// Fixture JSON path.
        fixture: String,
        /// Output directory.
        #[arg(long)]
        out: String,
    },
    /// Write an audit artifact bundle from flat window JSONL.
    AuditWindows {
        /// Window JSONL path.
        windows: String,
        /// Window name to use when rows omit `windowName`.
        #[arg(long)]
        window: Option<String>,
        /// Target source.
        #[arg(long)]
        target: String,
        /// Against source. May be repeated.
        #[arg(long)]
        against: Vec<String>,
        /// Output directory.
        #[arg(long)]
        out: String,
    },
}

/// Supported comparison output formats.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    /// Deterministic JSON.
    Json,
    /// Deterministic Markdown.
    Markdown,
    /// Deterministic LLM context JSON.
    LlmContext,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => code,
        Err(message) => {
            eprintln!(
                "{{\"error\":{}}}",
                serde_json::to_string(&message).expect("valid json")
            );
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<ExitCode, String> {
    match cli.command {
        Command::ValidatePlan { fixture } => {
            let fixture = load_fixture(&fixture)?;
            let result = fixture.execute();
            let payload = serde_json::json!({
                "isValid": result.is_valid,
                "diagnostics": result.diagnostics.into_iter().map(|item| item.code).collect::<Vec<_>>(),
            });
            println!(
                "{}",
                serde_json::to_string(&payload).map_err(|error| error.to_string())?
            );
            Ok(if result.is_valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Command::Compare { fixture, format } => {
            let fixture = load_fixture(&fixture)?;
            let result = fixture.execute();
            let format = match format {
                OutputFormat::Json => {
                    export_result_json(&result).map_err(|error| error.to_string())?
                }
                OutputFormat::Markdown => export_result_markdown(&result),
                OutputFormat::LlmContext => {
                    export_result_llm_context(&result).map_err(|error| error.to_string())?
                }
            };
            println!("{format}");
            Ok(if result.is_valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Command::Explain { fixture } => {
            let fixture = load_fixture(&fixture)?;
            println!("{}", export_result_markdown(&fixture.execute()));
            Ok(ExitCode::SUCCESS)
        }
        Command::Audit { fixture, out } => {
            let fixture = load_fixture(&fixture)?;
            let result = fixture.execute();
            write_audit_bundle(&result, &out)?;
            Ok(if result.is_valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Command::AuditWindows {
            windows,
            window,
            target,
            against,
            out,
        } => {
            let result = compare_windows_jsonl(&windows, window.as_deref(), &target, &against)?;
            write_audit_bundle(&result, &out)?;
            Ok(if result.is_valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
    }
}

fn load_fixture(path: &str) -> Result<ContractFixture, String> {
    let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
    ContractFixture::parse_json(&json).map_err(|error| error.to_string())
}

fn write_audit_bundle(result: &spanfold::ComparisonResult, out: &str) -> Result<(), String> {
    fs::create_dir_all(out).map_err(|error| error.to_string())?;
    let json = export_result_json(result).map_err(|error| error.to_string())?;
    let markdown = export_result_markdown(result);
    let llm = export_result_llm_context(result).map_err(|error| error.to_string())?;
    let html = export_result_debug_html(result);
    fs::write(format!("{out}/comparison.json"), &json).map_err(|error| error.to_string())?;
    fs::write(format!("{out}/comparison.md"), &markdown).map_err(|error| error.to_string())?;
    fs::write(format!("{out}/comparison.llm.json"), &llm).map_err(|error| error.to_string())?;
    fs::write(format!("{out}/comparison.html"), html).map_err(|error| error.to_string())?;
    let manifest = serde_json::json!({
        "schema": "spanfold.audit.bundle",
        "schemaVersion": 0,
        "artifact": "audit-bundle",
        "planName": result.plan_name,
        "isValid": result.is_valid,
        "diagnosticCount": result.diagnostics.len(),
        "provisionalRowCount": result.row_finalities.iter().filter(|item| item.finality == ComparisonFinality::Provisional).count(),
        "rowCounts": row_counts_json(result),
        "artifacts": {
            "json": "comparison.json",
            "markdown": "comparison.md",
            "debugHtml": "comparison.html",
            "llmContext": "comparison.llm.json",
            "manifest": "manifest.json"
        }
    });
    let manifest = serde_json::to_string_pretty(&manifest).map_err(|error| error.to_string())?;
    fs::write(format!("{out}/manifest.json"), &manifest).map_err(|error| error.to_string())?;
    println!("{manifest}");
    Ok(())
}

fn row_counts_json(result: &spanfold::ComparisonResult) -> serde_json::Value {
    serde_json::json!({
        "overlap": result.overlap_rows.len(),
        "residual": result.residual_rows.len(),
        "missing": result.missing_rows.len(),
        "coverage": result.coverage_rows.len(),
        "gap": result.gap_rows.len(),
        "symmetricDifference": result.symmetric_difference_rows.len(),
        "containment": result.containment_rows.len(),
        "leadLag": result.lead_lag_rows.len(),
        "asOf": result.as_of_rows.len()
    })
}

fn compare_windows_jsonl(
    path: &str,
    default_window_name: Option<&str>,
    target: &str,
    against: &[String],
) -> Result<spanfold::ComparisonResult, String> {
    if against.is_empty() {
        return Err("audit-windows requires at least one --against value".to_owned());
    }
    let lines = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let mut builder = WindowHistoryFixture::new();
    for (index, line) in lines.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row: JsonlWindow =
            serde_json::from_str(line).map_err(|error| format!("{path}:{}: {error}", index + 1))?;
        let window_name = row
            .window_name
            .as_deref()
            .or(default_window_name)
            .ok_or_else(|| {
                format!(
                    "{path}:{}: windowName missing and --window not supplied",
                    index + 1
                )
            })?;
        let key = row.key.clone();
        let resolved_window_name = window_name.to_owned();
        if let Some(end) = row.end_position {
            builder = builder
                .closed_window(
                    resolved_window_name.clone(),
                    key.clone(),
                    row.start_position,
                    end,
                    |w| apply_jsonl_metadata(w, &row),
                )
                .map_err(|error| error.to_string())?;
        } else {
            builder = builder.open_window(resolved_window_name, key, row.start_position, |w| {
                apply_jsonl_metadata(w, &row)
            });
        }
    }
    let history = builder.build();
    let plan = ComparisonPlan {
        name: "Spanfold Window Audit".to_owned(),
        target_source: target.to_owned(),
        against: AgainstSelection::Sources(against.to_vec()),
        scope_window: default_window_name.map(str::to_owned),
        scope_segments: Vec::new(),
        scope_tags: Vec::new(),
        comparators: vec![
            Comparator::Overlap,
            Comparator::Residual,
            Comparator::Missing,
            Comparator::Coverage,
            Comparator::Gap,
            Comparator::SymmetricDifference,
        ],
        known_at: None,
        open_window_policy: OpenWindowPolicy::RequireClosed,
        open_window_horizon: None,
        strict: false,
    };
    Ok(compare(&history, &plan))
}

fn apply_jsonl_metadata(
    mut builder: spanfold::WindowHistoryFixtureWindow,
    row: &JsonlWindow,
) -> spanfold::WindowHistoryFixtureWindow {
    builder = builder.source(row.source.clone());
    if let Some(partition) = &row.partition {
        builder = builder.partition(partition.clone());
    }
    for segment in &row.segments {
        builder = if let Some(parent) = &segment.parent_name {
            builder.child_segment(segment.name.clone(), segment.value.clone(), parent.clone())
        } else {
            builder.segment(segment.name.clone(), segment.value.clone())
        };
    }
    for tag in &row.tags {
        builder = builder.tag(tag.name.clone(), tag.value.clone());
    }
    builder
}

#[derive(Clone, Debug, Deserialize)]
struct JsonlWindow {
    #[serde(rename = "windowName")]
    window_name: Option<String>,
    key: String,
    source: String,
    partition: Option<String>,
    #[serde(rename = "startPosition")]
    start_position: i64,
    #[serde(rename = "endPosition")]
    end_position: Option<i64>,
    #[serde(default)]
    segments: Vec<JsonlNamedValue>,
    #[serde(default)]
    tags: Vec<JsonlNamedValue>,
}

#[derive(Clone, Debug, Deserialize)]
struct JsonlNamedValue {
    name: String,
    value: PrimitiveValue,
    #[serde(rename = "parentName")]
    parent_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn audit_windows_supports_basic_jsonl_windows() {
        let mut file = NamedTempFile::new().expect("temp file");
        writeln!(
            file,
            "{{\"key\":\"device-1\",\"source\":\"provider-a\",\"startPosition\":1,\"endPosition\":5}}"
        )
        .expect("write first row");
        writeln!(
            file,
            "{{\"key\":\"device-1\",\"source\":\"provider-b\",\"startPosition\":3,\"endPosition\":7}}"
        )
        .expect("write second row");

        let result = compare_windows_jsonl(
            file.path().to_str().expect("utf8 path"),
            Some("DeviceOffline"),
            "provider-a",
            &[String::from("provider-b")],
        )
        .expect("jsonl compare");

        assert!(result.is_valid);
        assert_eq!(result.overlap_rows.len(), 1);
        assert_eq!(result.residual_rows.len(), 1);
        assert_eq!(result.coverage_rows.len(), 2);
        assert_eq!(result.missing_rows.len(), 1);
        assert_eq!(result.gap_rows.len(), 0);
        assert_eq!(result.symmetric_difference_rows.len(), 2);
    }
}
