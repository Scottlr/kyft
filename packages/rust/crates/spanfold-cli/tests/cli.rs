use std::{fs, path::PathBuf};

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

fn fixture_path(name: &str) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.join("../../../dotnet/tests/Spanfold.Tests/Comparison/Fixtures")
        .join(name)
}

#[test]
fn compare_outputs_json_for_basic_overlap_fixture() {
    Command::cargo_bin("spanfold")
        .expect("binary")
        .args([
            "compare",
            fixture_path("basic-overlap.json")
                .to_str()
                .expect("utf8 fixture path"),
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"schema\": \"spanfold.comparison.result\"",
        ))
        .stdout(predicate::str::contains("\"rowCount\": 1"));
}

#[test]
fn audit_writes_artifact_bundle() {
    let out = tempdir().expect("tempdir");
    Command::cargo_bin("spanfold")
        .expect("binary")
        .args([
            "audit",
            fixture_path("basic-overlap.json")
                .to_str()
                .expect("utf8 fixture path"),
            "--out",
            out.path().to_str().expect("utf8 output path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"schema\": \"spanfold.audit.bundle\"",
        ));

    assert!(fs::exists(out.path().join("comparison.json")).expect("comparison.json status"));
    assert!(fs::exists(out.path().join("comparison.md")).expect("comparison.md status"));
    assert!(fs::exists(out.path().join("comparison.html")).expect("comparison.html status"));
    assert!(
        fs::exists(out.path().join("comparison.llm.json")).expect("comparison.llm.json status")
    );
    assert!(fs::exists(out.path().join("manifest.json")).expect("manifest.json status"));
}

#[test]
fn compare_outputs_llm_context_with_row_documents() {
    Command::cargo_bin("spanfold")
        .expect("binary")
        .args([
            "compare",
            fixture_path("basic-overlap.json")
                .to_str()
                .expect("utf8 fixture path"),
            "--format",
            "llm-context",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"schema\": \"spanfold.comparison.llm-context\"",
        ))
        .stdout(predicate::str::contains("\"artifact\": \"result-summary\""))
        .stdout(predicate::str::contains("\"rowId\": \"overlap[0]\""));
}
