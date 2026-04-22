"""Command-line fixture runner for Spanfold comparison contracts."""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TextIO

from spanfold.comparison import CohortActivity
from spanfold.records import WindowHistory, WindowSegment, WindowTag
from spanfold.temporal import TemporalPoint
from spanfold.testing import WindowHistoryFixtureBuilder


def run(args: Sequence[str], stdout: TextIO, stderr: TextIO) -> int:
    """Run the Spanfold fixture CLI and return a process exit code."""

    try:
        if len(args) < 2:
            _write_error(
                stderr,
                "Usage: spanfold <validate-plan|compare|explain> "
                "<fixture.json> [--format json|markdown|llm-context]",
            )
            return 2
        command = args[0]
        if command not in {"validate-plan", "compare", "explain"}:
            _write_error(stderr, f"Unknown command: {command}")
            return 2
        output_format = _read_format(args)
        fixture = json.loads(Path(args[1]).read_text(encoding="utf-8"))
        _validate_fixture(fixture)
        result = _execute_fixture(fixture)

        if command == "validate-plan":
            stdout.write(
                json.dumps(
                    {
                        "isValid": result.is_valid,
                        "diagnostics": [item.code for item in result.diagnostic_rows],
                    },
                    separators=(",", ":"),
                )
            )
            return 0 if result.is_valid else 1
        if command == "compare":
            if output_format == "markdown":
                stdout.write(result.export_markdown())
            elif output_format == "llm-context":
                stdout.write(result.export_llm_context())
            else:
                stdout.write(result.export_json())
            return 0 if result.is_valid else 1
        stdout.write(result.export_markdown())
        return 0 if result.is_valid else 1
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exception:
        _write_error(stderr, str(exception))
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    """Console-script entry point."""

    return run(sys.argv[1:] if argv is None else argv, sys.stdout, sys.stderr)


def _read_format(args: Sequence[str]) -> str:
    for index in range(2, len(args) - 1):
        if args[index] != "--format":
            continue
        output_format = args[index + 1]
        if output_format not in {"json", "markdown", "llm-context"}:
            msg = f"Unsupported format: {output_format}"
            raise ValueError(msg)
        return output_format
    return "json"


def _execute_fixture(fixture: dict[str, Any]) -> Any:
    history = _create_history(fixture["windows"])
    plan = fixture["plan"]
    builder = (
        history.compare(plan["name"])
        .target(plan["targetSource"])
        .within(
            window_name=plan.get("scopeWindow"),
            segments={item["name"]: item["value"] for item in plan.get("scopeSegments", ())},
            tags={item["name"]: item["value"] for item in plan.get("scopeTags", ())},
        )
        .using(*plan["comparators"])
    )
    for source in plan.get("againstSources", ()):
        builder.against(source)
    cohort = plan.get("againstCohort")
    if cohort is not None:
        builder.against_cohort(
            cohort["name"],
            sources=cohort["sources"],
            activity=_read_cohort_activity(cohort),
        )
    if plan.get("strict", False):
        builder.strict()
    horizon = plan.get("liveHorizonPosition")
    if horizon is not None:
        return builder.run_live(TemporalPoint.for_position(int(horizon)))
    return builder.run()


def _create_history(windows: list[dict[str, Any]]) -> WindowHistory:
    builder = WindowHistoryFixtureBuilder()
    for window in windows:
        source = window.get("source")
        partition = window.get("partition")
        segments = _read_segments(window)
        tags = _read_tags(window)
        if window["endPosition"] is None:
            builder.add_open_window(
                window["windowName"],
                window["key"],
                int(window["startPosition"]),
                source=source,
                partition=partition,
                segments=segments,
                tags=tags,
            )
        else:
            builder.add_closed_window(
                window["windowName"],
                window["key"],
                int(window["startPosition"]),
                int(window["endPosition"]),
                source=source,
                partition=partition,
                segments=segments,
                tags=tags,
            )
    return builder.build()


def _read_segments(window: dict[str, Any]) -> tuple[WindowSegment, ...]:
    return tuple(
        WindowSegment(item["name"], item.get("value"), item.get("parentName"))
        for item in window.get("segments", ())
    )


def _read_tags(window: dict[str, Any]) -> tuple[WindowTag, ...]:
    return tuple(WindowTag(item["name"], item.get("value")) for item in window.get("tags", ()))


def _read_cohort_activity(cohort: dict[str, Any]) -> CohortActivity:
    activity = cohort.get("activity", "any")
    count = cohort.get("count")
    if activity == "any":
        return CohortActivity.any()
    if activity == "all":
        return CohortActivity.all()
    if activity == "none":
        return CohortActivity.none()
    if activity == "at-least":
        if count is None:
            raise ValueError("Cohort count is required for at-least activity.")
        return CohortActivity.at_least(int(count))
    if activity == "at-most":
        if count is None:
            raise ValueError("Cohort count is required for at-most activity.")
        return CohortActivity.at_most(int(count))
    if activity == "exactly":
        if count is None:
            raise ValueError("Cohort count is required for exactly activity.")
        return CohortActivity.exactly(int(count))
    msg = f"Unsupported cohort activity: {activity}"
    raise ValueError(msg)


def _validate_fixture(fixture: Any) -> None:
    if not isinstance(fixture, dict):
        raise ValueError("$ must be an object.")
    _require(fixture, "schema", str, "$")
    _require(fixture, "schemaVersion", int, "$")
    windows = _require(fixture, "windows", list, "$")
    plan = _require(fixture, "plan", dict, "$")
    for index, window in enumerate(windows):
        _validate_window(window, f"$.windows[{index}]")
    _validate_plan(plan)


def _validate_window(window: Any, path: str) -> None:
    if not isinstance(window, dict):
        raise ValueError(f"{path} must be an object.")
    _require(window, "windowName", str, path)
    _require(window, "key", str, path)
    _require(window, "source", str, path)
    start = _require(window, "startPosition", int, path)
    end = window.get("endPosition")
    if end is not None and not isinstance(end, int):
        raise ValueError(f"{path}.endPosition must be a number or null.")
    if end is not None and end < start:
        raise ValueError(f"{path}.endPosition must be greater than or equal to startPosition.")
    _validate_named_values(window.get("segments", ()), f"{path}.segments", allow_parent=True)
    _validate_named_values(window.get("tags", ()), f"{path}.tags", allow_parent=False)


def _validate_plan(plan: dict[str, Any]) -> None:
    _require(plan, "name", str, "$.plan")
    _require(plan, "targetSource", str, "$.plan")
    has_sources = "againstSources" in plan
    has_cohort = plan.get("againstCohort") is not None
    if not has_sources and not has_cohort:
        raise ValueError("$.plan must contain againstSources or againstCohort.")
    if has_sources:
        sources = _require(plan, "againstSources", list, "$.plan")
        if not sources and not has_cohort:
            raise ValueError("$.plan.againstSources must contain at least one source.")
        if not all(isinstance(source, str) for source in sources):
            raise ValueError("$.plan.againstSources entries must be strings.")
    if has_cohort:
        cohort = _require(plan, "againstCohort", dict, "$.plan")
        _require(cohort, "name", str, "$.plan.againstCohort")
        sources = _require(cohort, "sources", list, "$.plan.againstCohort")
        if not sources:
            raise ValueError("$.plan.againstCohort.sources must contain at least one source.")
    if plan.get("scopeWindow") is not None and not isinstance(plan["scopeWindow"], str):
        raise ValueError("$.plan.scopeWindow must be a string or null.")
    _validate_named_values(
        plan.get("scopeSegments", ()),
        "$.plan.scopeSegments",
        allow_parent=False,
    )
    _validate_named_values(plan.get("scopeTags", ()), "$.plan.scopeTags", allow_parent=False)
    comparators = _require(plan, "comparators", list, "$.plan")
    if not comparators:
        raise ValueError("$.plan.comparators must contain at least one comparator.")
    if not all(isinstance(comparator, str) for comparator in comparators):
        raise ValueError("$.plan.comparators entries must be strings.")
    _require(plan, "strict", bool, "$.plan")


def _validate_named_values(values: Any, path: str, *, allow_parent: bool) -> None:
    if values == ():
        return
    if not isinstance(values, list):
        raise ValueError(f"{path} must be an array.")
    for index, item in enumerate(values):
        item_path = f"{path}[{index}]"
        if not isinstance(item, dict):
            raise ValueError(f"{item_path} must be an object.")
        _require(item, "name", str, item_path)
        if "value" not in item:
            raise ValueError(f"{item_path}.value is required.")
        if not allow_parent and "parentName" in item:
            raise ValueError(f"{item_path}.parentName is not supported here.")


def _require(container: dict[str, Any], key: str, kind: type[Any], path: str) -> Any:
    if key not in container:
        raise ValueError(f"{path}.{key} is required.")
    value = container[key]
    if not isinstance(value, kind):
        raise ValueError(f"{path}.{key} must be {kind.__name__}.")
    return value


def _write_error(writer: TextIO, message: str) -> None:
    writer.write(json.dumps({"error": message}, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
