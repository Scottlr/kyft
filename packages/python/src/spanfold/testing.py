"""Testing helpers for Spanfold fixtures, snapshots, and assertions."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from spanfold.comparison import ComparisonResult
from spanfold.records import (
    ClosedWindow,
    OpenWindow,
    WindowHistory,
    WindowSegment,
    WindowTag,
)
from spanfold.temporal import TemporalPoint


class SpanfoldAssertionError(AssertionError):
    """Framework-neutral assertion error raised by Spanfold test helpers."""


@dataclass(slots=True)
class WindowHistoryFixtureWindowBuilder:
    """Builds one recorded window for a window-history fixture."""

    source_value: object | None = None
    partition_value: object | None = None
    segments: list[WindowSegment] = field(default_factory=list)
    tags: list[WindowTag] = field(default_factory=list)

    def source(self, source: object) -> WindowHistoryFixtureWindowBuilder:
        """Set the source identity."""

        self.source_value = source
        return self

    def partition(self, partition: object) -> WindowHistoryFixtureWindowBuilder:
        """Set the partition identity."""

        self.partition_value = partition
        return self

    def segment(
        self,
        name: str,
        value: object,
        *,
        parent_name: str | None = None,
    ) -> WindowHistoryFixtureWindowBuilder:
        """Add an analytical segment value."""

        self.segments.append(WindowSegment(name, value, parent_name))
        return self

    def tag(self, name: str, value: object) -> WindowHistoryFixtureWindowBuilder:
        """Add a descriptive tag."""

        self.tags.append(WindowTag(name, value))
        return self


class WindowHistoryFixtureBuilder:
    """Builds small window histories without running a full event pipeline."""

    def __init__(self) -> None:
        self._closed: list[ClosedWindow] = []
        self._open: list[OpenWindow] = []

    def add_closed_window(
        self,
        window_name: str,
        key: object,
        start_position: int,
        end_position: int,
        *,
        source: object | None = None,
        partition: object | None = None,
        segments: list[WindowSegment] | tuple[WindowSegment, ...] | None = None,
        tags: list[WindowTag] | tuple[WindowTag, ...] | None = None,
    ) -> WindowHistoryFixtureBuilder:
        """Add a closed window to the fixture history."""

        self._closed.append(
            ClosedWindow(
                window_name,
                key,
                start_position,
                end_position,
                source,
                partition,
                segments=segments,
                tags=tags,
            )
        )
        return self

    def add_open_window(
        self,
        window_name: str,
        key: object,
        start_position: int,
        *,
        source: object | None = None,
        partition: object | None = None,
        segments: list[WindowSegment] | tuple[WindowSegment, ...] | None = None,
        tags: list[WindowTag] | tuple[WindowTag, ...] | None = None,
    ) -> WindowHistoryFixtureBuilder:
        """Add an open window to the fixture history."""

        self._open.append(
            OpenWindow(
                window_name,
                key,
                start_position,
                source,
                partition,
                segments=segments,
                tags=tags,
            )
        )
        return self

    def closed_window(
        self,
        window_name: str,
        key: object,
        start_position: int,
        end_position: int,
        configure: object,
    ) -> WindowHistoryFixtureBuilder:
        """Add a closed window using a chainable window builder."""

        builder = WindowHistoryFixtureWindowBuilder()
        configure(builder)  # type: ignore[operator]
        return self.add_closed_window(
            window_name,
            key,
            start_position,
            end_position,
            source=builder.source_value,
            partition=builder.partition_value,
            segments=builder.segments,
            tags=builder.tags,
        )

    def build(self) -> WindowHistory:
        """Build a window history fixture."""

        history = WindowHistory(enabled=True)
        history._closed.extend(self._closed)  # noqa: SLF001
        for window in self._open:
            history.record_open(window)
        return history


class SpanfoldAssert:
    """Framework-neutral assertions for Spanfold comparison artifacts."""

    @staticmethod
    def is_valid(result: ComparisonResult) -> None:
        """Assert that a comparison result has no diagnostics."""

        if result.diagnostics:
            raise SpanfoldAssertionError("Expected a valid Spanfold result.")

    @staticmethod
    def has_no_diagnostics(result: ComparisonResult) -> None:
        """Assert that a comparison result contains no diagnostics."""

        if result.diagnostics:
            raise SpanfoldAssertionError(
                f"Expected no Spanfold diagnostics, found {len(result.diagnostics)}."
            )

    @staticmethod
    def has_row_count(result: ComparisonResult, row_type: str, expected_count: int) -> None:
        """Assert that a named row collection contains an expected count."""

        actual = _row_count(result, row_type)
        if actual != expected_count:
            raise SpanfoldAssertionError(
                f"Expected {expected_count} {row_type} rows, found {actual}."
            )


class SpanfoldSnapshot:
    """Snapshot normalization helpers for Spanfold artifacts."""

    @staticmethod
    def normalize(value: str, *, normalize_record_ids: bool = True) -> str:
        """Normalize line endings, trailing whitespace, and record IDs."""

        normalized = value.replace("\r\n", "\n").replace("\r", "\n").rstrip()
        if normalize_record_ids:
            normalized = _normalize_record_ids(normalized)
        return normalized + "\n"

    @staticmethod
    def assert_equal(expected: str, actual: str) -> None:
        """Assert that two snapshot strings are equal after normalization."""

        normalized_expected = SpanfoldSnapshot.normalize(expected)
        normalized_actual = SpanfoldSnapshot.normalize(actual)
        if normalized_expected != normalized_actual:
            raise SpanfoldAssertionError("Spanfold snapshot mismatch.")


class VirtualComparisonClock:
    """Deterministic processing-position horizon for live comparison tests."""

    def __init__(self, initial_position: int = 0) -> None:
        if initial_position < 0:
            raise ValueError("Initial position cannot be negative.")
        self.position = initial_position

    @property
    def horizon(self) -> TemporalPoint:
        """Return the current processing-position horizon."""

        return TemporalPoint.for_position(self.position)

    def advance_by(self, positions: int) -> TemporalPoint:
        """Advance by a non-negative position delta."""

        if positions < 0:
            raise ValueError("Position delta cannot be negative.")
        self.position += positions
        return self.horizon

    def advance_to(self, position: int) -> TemporalPoint:
        """Advance to an absolute processing position."""

        if position < self.position:
            raise ValueError("Virtual comparison clocks cannot move backwards.")
        self.position = position
        return self.horizon


def _row_count(result: ComparisonResult, row_type: str) -> int:
    normalized = row_type.replace("-", "_").lower()
    rows = {
        "overlap": result.overlap_rows,
        "residual": result.residual_rows,
        "missing": result.missing_rows,
        "coverage": result.coverage_rows,
        "gap": result.gap_rows,
        "symmetric_difference": result.symmetric_difference_rows,
        "containment": result.containment_rows,
        "lead_lag": result.lead_lag_rows,
        "as_of": result.as_of_rows,
    }.get(normalized)
    if rows is None:
        raise ValueError(f"Unknown Spanfold row type: {row_type}")
    return len(rows)


def _normalize_record_ids(value: str) -> str:
    ids: dict[str, str] = {}

    def replace(match: re.Match[str]) -> str:
        if match.group(0) not in ids:
            ids[match.group(0)] = f"<record-id:{len(ids) + 1}>"
        return ids[match.group(0)]

    return re.sub(r"\b[a-f0-9]{16,64}\b", replace, value)
