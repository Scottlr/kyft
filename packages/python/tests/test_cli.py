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


def test_cli_reports_unknown_command() -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = run(["unknown", str(FIXTURE_DIR / "basic-overlap.json")], stdout, stderr)

    assert exit_code == 2
    assert json.loads(stderr.getvalue())["error"] == "Unknown command: unknown"
    assert stdout.getvalue() == ""
