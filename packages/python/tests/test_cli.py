import io
import json
from pathlib import Path

from spanfold.cli import run

FIXTURE_DIR = (
    Path(__file__).parents[2]
    / "dotnet"
    / "tests"
    / "Spanfold.Tests"
    / "Comparison"
    / "Fixtures"
)


def test_cli_validate_plan_accepts_valid_fixture() -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = run(["validate-plan", str(FIXTURE_DIR / "basic-overlap.json")], stdout, stderr)

    assert exit_code == 0
    assert json.loads(stdout.getvalue())["isValid"] is True
    assert stderr.getvalue() == ""


def test_cli_compare_outputs_json_and_llm_context() -> None:
    fixture = str(FIXTURE_DIR / "basic-overlap.json")
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = run(["compare", fixture, "--format", "json"], stdout, stderr)

    assert exit_code == 0
    assert json.loads(stdout.getvalue())["schema"] == "spanfold.comparison.result"
    assert stderr.getvalue() == ""

    stdout = io.StringIO()
    stderr = io.StringIO()
    exit_code = run(["compare", fixture, "--format", "llm-context"], stdout, stderr)

    assert exit_code == 0
    payload = json.loads(stdout.getvalue())
    assert payload["schema"] == "spanfold.comparison.llm-context"
    assert payload["fullResult"]["schema"] == "spanfold.comparison.result"


def test_cli_audit_writes_artifact_bundle(tmp_path: Path) -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = run(
        ["audit", str(FIXTURE_DIR / "basic-overlap.json"), "--out", str(tmp_path)],
        stdout,
        stderr,
    )

    assert exit_code == 0
    assert stderr.getvalue() == ""
    assert (tmp_path / "comparison.json").is_file()
    assert (tmp_path / "comparison.md").is_file()
    assert (tmp_path / "comparison.html").is_file()
    assert (tmp_path / "comparison.llm.json").is_file()
    assert (tmp_path / "manifest.json").is_file()
    assert stdout.getvalue() == (tmp_path / "manifest.json").read_text(encoding="utf-8")
    payload = json.loads(stdout.getvalue())
    assert payload["schema"] == "spanfold.audit.bundle"
    assert payload["artifact"] == "audit-bundle"
    assert payload["isValid"] is True
    assert payload["rowCounts"]["overlap"] == 1
    assert payload["artifacts"]["llmContext"] == "comparison.llm.json"


def test_cli_audit_windows_accepts_json_lines_without_fixture_plan(tmp_path: Path) -> None:
    windows = tmp_path / "windows.jsonl"
    windows.write_text(
        "\n".join(
            [
                '{"key":"device-1","source":"provider-a","startPosition":1,"endPosition":5}',
                '{"key":"device-1","source":"provider-b","startPosition":3,"endPosition":7}',
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "audit"
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = run(
        [
            "audit-windows",
            str(windows),
            "--window",
            "DeviceOffline",
            "--target",
            "provider-a",
            "--against",
            "provider-b",
            "--out",
            str(output_dir),
        ],
        stdout,
        stderr,
    )

    assert exit_code == 0
    assert stderr.getvalue() == ""
    assert (output_dir / "comparison.llm.json").is_file()
    payload = json.loads(stdout.getvalue())
    assert payload["schema"] == "spanfold.audit.bundle"
    assert payload["planName"] == "Spanfold Window Audit"
    assert payload["rowCounts"]["overlap"] == 1
    assert payload["rowCounts"]["residual"] == 1


def test_cli_reports_unknown_command() -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = run(["unknown", str(FIXTURE_DIR / "basic-overlap.json")], stdout, stderr)

    assert exit_code == 2
    assert json.loads(stderr.getvalue())["error"] == "Unknown command: unknown"
    assert stdout.getvalue() == ""
