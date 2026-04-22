"""Window comparison planning, row models, and deterministic exports."""

from __future__ import annotations

import html
import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from spanfold.records import ClosedWindow, WindowHistory, WindowRecord, WindowRecordId
from spanfold.temporal import TemporalAxis, TemporalPoint, TemporalRange


class ComparisonFinality(Enum):
    """Describes whether a comparison row is final or horizon-provisional."""

    FINAL = "final"
    PROVISIONAL = "provisional"
    REVISED = "revised"
    RETRACTED = "retracted"


class ComparisonDiagnosticSeverity(Enum):
    """Describes whether a comparison diagnostic is advisory or blocking."""

    WARNING = "warning"
    ERROR = "error"


class ComparisonSide(Enum):
    """Identifies which side is active for a directional comparison row."""

    TARGET = "target"
    AGAINST = "against"


class ComparisonOpenWindowPolicy(Enum):
    """Describes how open windows are handled during normalization."""

    REQUIRE_CLOSED = "require_closed"
    CLIP_TO_HORIZON = "clip_to_horizon"


class ComparisonNullTimestampPolicy(Enum):
    """Describes how event-time normalization handles missing timestamps."""

    REJECT = "reject"
    EXCLUDE = "exclude"


class ComparisonDuplicateWindowPolicy(Enum):
    """Describes how duplicate normalized windows are handled."""

    PRESERVE = "preserve"
    REJECT = "reject"


class ContainmentStatus(Enum):
    """Describes whether target windows are contained by comparison windows."""

    CONTAINED = "contained"
    NOT_CONTAINED = "not_contained"
    LEFT_OVERHANG = "left_overhang"
    RIGHT_OVERHANG = "right_overhang"


class LeadLagTransition(Enum):
    """Describes which window transition is used for lead/lag measurement."""

    START = "start"
    END = "end"


class LeadLagDirection(Enum):
    """Describes the ordering relationship between target and comparison."""

    EQUAL = "equal"
    TARGET_LEADS = "target_leads"
    TARGET_LAGS = "target_lags"
    MISSING_COMPARISON = "missing_comparison"


class AsOfDirection(Enum):
    """Describes which comparison transition is eligible for as-of lookup."""

    PREVIOUS = "previous"
    NEXT = "next"
    NEAREST = "nearest"


class AsOfMatchStatus(Enum):
    """Describes the outcome of an as-of lookup."""

    EXACT = "exact"
    MATCHED = "matched"
    NO_MATCH = "no_match"
    FUTURE_REJECTED = "future_rejected"
    AMBIGUOUS = "ambiguous"


class HierarchyComparisonRowKind(Enum):
    """Describes how a parent/child hierarchy segment is interpreted."""

    PARENT_EXPLAINED = "parent_explained"
    UNEXPLAINED_PARENT = "unexplained_parent"
    ORPHAN_CHILD = "orphan_child"


@dataclass(frozen=True, slots=True)
class ComparisonNormalizationPolicy:
    """Describes how recorded windows are normalized before comparison."""

    require_closed_windows: bool = True
    use_half_open_ranges: bool = True
    time_axis: TemporalAxis = TemporalAxis.PROCESSING_POSITION
    open_window_policy: ComparisonOpenWindowPolicy = ComparisonOpenWindowPolicy.REQUIRE_CLOSED
    open_window_horizon: TemporalPoint | None = None
    null_timestamp_policy: ComparisonNullTimestampPolicy = ComparisonNullTimestampPolicy.REJECT
    coalesce_adjacent_windows: bool = False
    duplicate_window_policy: ComparisonDuplicateWindowPolicy = (
        ComparisonDuplicateWindowPolicy.PRESERVE
    )
    known_at: TemporalPoint | None = None

    def __post_init__(self) -> None:
        if not self.use_half_open_ranges:
            msg = "Only half-open ranges are supported."
            raise ValueError(msg)
        if (
            self.open_window_policy is ComparisonOpenWindowPolicy.CLIP_TO_HORIZON
            and self.open_window_horizon is None
        ):
            msg = "open_window_horizon is required when clipping open windows."
            raise ValueError(msg)
        if (
            self.open_window_policy is ComparisonOpenWindowPolicy.REQUIRE_CLOSED
            and self.open_window_horizon is not None
        ):
            msg = "open_window_horizon cannot be set when open windows require closure."
            raise ValueError(msg)

    @classmethod
    def default(cls) -> ComparisonNormalizationPolicy:
        """Return the default historical comparison normalization policy."""

        return cls()

    @classmethod
    def require_closed(cls) -> ComparisonNormalizationPolicy:
        """Return a policy that excludes open windows from historical comparison."""

        return cls()

    @classmethod
    def clip_open_windows_to(
        cls,
        horizon: TemporalPoint,
        *,
        time_axis: TemporalAxis | None = None,
    ) -> ComparisonNormalizationPolicy:
        """Return a policy that clips open windows to an explicit horizon."""

        return cls(
            require_closed_windows=False,
            time_axis=time_axis or horizon.axis,
            open_window_policy=ComparisonOpenWindowPolicy.CLIP_TO_HORIZON,
            open_window_horizon=horizon,
        )

    @classmethod
    def event_time(
        cls,
        *,
        null_timestamp_policy: ComparisonNullTimestampPolicy = (
            ComparisonNullTimestampPolicy.REJECT
        ),
    ) -> ComparisonNormalizationPolicy:
        """Return a policy that normalizes on the event-time axis."""

        return cls(
            time_axis=TemporalAxis.TIMESTAMP,
            null_timestamp_policy=null_timestamp_policy,
        )

    def with_known_at(self, point: TemporalPoint) -> ComparisonNormalizationPolicy:
        """Return this policy with a known-at availability point."""

        return self._replace(known_at=point)

    def coalescing_adjacent_windows(self) -> ComparisonNormalizationPolicy:
        """Return this policy with adjacent-window coalescing enabled."""

        return self._replace(coalesce_adjacent_windows=True)

    def rejecting_duplicate_windows(self) -> ComparisonNormalizationPolicy:
        """Return this policy with duplicate normalized windows rejected."""

        return self._replace(
            duplicate_window_policy=ComparisonDuplicateWindowPolicy.REJECT
        )

    def _replace(self, **changes: Any) -> ComparisonNormalizationPolicy:
        return replace(self, **changes)


@dataclass(frozen=True, slots=True)
class CohortActivity:
    """Rule describing when a source cohort is considered active."""

    name: str
    count: int | None = None

    @classmethod
    def any(cls) -> CohortActivity:
        """Return an any-member activity rule."""

        return cls("any")

    @classmethod
    def all(cls) -> CohortActivity:
        """Return an all-members activity rule."""

        return cls("all")

    @classmethod
    def none(cls) -> CohortActivity:
        """Return a no-active-members activity rule."""

        return cls("none")

    @classmethod
    def at_least(cls, count: int) -> CohortActivity:
        """Return an at-least activity rule."""

        if count < 1:
            raise ValueError("At-least cohort count must be greater than zero.")
        return cls("at_least", count)

    @classmethod
    def at_most(cls, count: int) -> CohortActivity:
        """Return an at-most activity rule."""

        if count < 0:
            raise ValueError("At-most cohort count cannot be negative.")
        return cls("at_most", count)

    @classmethod
    def exactly(cls, count: int) -> CohortActivity:
        """Return an exact-count activity rule."""

        if count < 0:
            raise ValueError("Exact cohort count cannot be negative.")
        return cls("exactly", count)

    def required_count(self, member_count: int) -> int:
        """Return the configured active-member count."""

        if self.name == "any":
            return 1
        if self.name == "all":
            return member_count
        if self.name == "none":
            return 0
        if self.count is None:
            raise ValueError("Cohort activity count is required.")
        return self.count

    def is_active(self, active_count: int, member_count: int) -> bool:
        """Return whether a segment satisfies this activity rule."""

        if self.name == "any":
            return active_count >= 1
        if self.name == "all":
            return active_count == member_count
        if self.name == "none":
            return active_count == 0
        if self.name == "at_least":
            return self.count is not None and active_count >= self.count
        if self.name == "at_most":
            return self.count is not None and active_count <= self.count
        if self.name == "exactly":
            return self.count is not None and active_count == self.count
        raise ValueError(f"Unknown cohort activity rule: {self.name}")


@dataclass(frozen=True, slots=True)
class OverlapRow:
    """Aligned segment where target and comparison windows overlap."""

    window_name: str
    key: Any
    partition: Any
    range: TemporalRange
    target_record_ids: tuple[WindowRecordId, ...]
    against_record_ids: tuple[WindowRecordId, ...]
    finality: ComparisonFinality = ComparisonFinality.FINAL


@dataclass(frozen=True, slots=True)
class ResidualRow:
    """Segment where target windows are active without comparison coverage."""

    window_name: str
    key: Any
    partition: Any
    range: TemporalRange
    target_record_ids: tuple[WindowRecordId, ...]
    finality: ComparisonFinality = ComparisonFinality.FINAL


@dataclass(frozen=True, slots=True)
class MissingRow:
    """Segment where comparison windows are active without target coverage."""

    window_name: str
    key: Any
    partition: Any
    range: TemporalRange
    against_record_ids: tuple[WindowRecordId, ...]
    finality: ComparisonFinality = ComparisonFinality.FINAL


@dataclass(frozen=True, slots=True)
class CoverageRow:
    """Target coverage for one aligned target segment."""

    window_name: str
    key: Any
    partition: Any
    range: TemporalRange
    target_magnitude: float
    covered_magnitude: float
    target_record_ids: tuple[WindowRecordId, ...]
    against_record_ids: tuple[WindowRecordId, ...]
    finality: ComparisonFinality = ComparisonFinality.FINAL

    @property
    def coverage_ratio(self) -> float | None:
        """Return covered magnitude divided by target magnitude."""

        if self.target_magnitude == 0:
            return None
        return self.covered_magnitude / self.target_magnitude


@dataclass(frozen=True, slots=True)
class GapRow:
    """Uncovered temporal space between observed comparison segments."""

    window_name: str
    key: Any
    partition: Any
    range: TemporalRange
    finality: ComparisonFinality = ComparisonFinality.FINAL


@dataclass(frozen=True, slots=True)
class SymmetricDifferenceRow:
    """Target-only or comparison-only disagreement segment."""

    window_name: str
    key: Any
    partition: Any
    range: TemporalRange
    side: ComparisonSide
    target_record_ids: tuple[WindowRecordId, ...]
    against_record_ids: tuple[WindowRecordId, ...]
    finality: ComparisonFinality = ComparisonFinality.FINAL


@dataclass(frozen=True, slots=True)
class ContainmentRow:
    """Directional target containment by comparison windows for one segment."""

    window_name: str
    key: Any
    partition: Any
    range: TemporalRange
    status: ContainmentStatus
    target_record_ids: tuple[WindowRecordId, ...]
    container_record_ids: tuple[WindowRecordId, ...]
    finality: ComparisonFinality = ComparisonFinality.FINAL


@dataclass(frozen=True, slots=True)
class LeadLagRow:
    """Lead or lag between a target transition and nearest comparison transition."""

    window_name: str
    key: Any
    partition: Any
    transition: LeadLagTransition
    axis: TemporalAxis
    target_point: TemporalPoint
    comparison_point: TemporalPoint | None
    delta_magnitude: int | None
    tolerance_magnitude: int
    is_within_tolerance: bool
    direction: LeadLagDirection
    target_record_id: WindowRecordId
    comparison_record_id: WindowRecordId | None
    finality: ComparisonFinality = ComparisonFinality.FINAL


@dataclass(frozen=True, slots=True)
class AsOfRow:
    """Point-in-time as-of lookup result."""

    window_name: str
    key: Any
    partition: Any
    axis: TemporalAxis
    direction: AsOfDirection
    target_point: TemporalPoint
    matched_point: TemporalPoint | None
    distance_magnitude: int | None
    tolerance_magnitude: int
    status: AsOfMatchStatus
    target_record_id: WindowRecordId
    matched_record_id: WindowRecordId | None
    finality: ComparisonFinality = ComparisonFinality.FINAL


@dataclass(frozen=True, slots=True)
class ComparatorSummary:
    """Summary row count for one comparator."""

    comparator: str
    row_count: int


@dataclass(frozen=True, slots=True)
class CoverageSummary:
    """Summarizes target coverage within one comparison scope."""

    window_name: str
    key: Any
    partition: Any
    target_magnitude: float
    covered_magnitude: float
    coverage_ratio: float


@dataclass(frozen=True, slots=True)
class ComparisonDiagnostic:
    """Structured diagnostic emitted while preparing or running a comparison."""

    code: str
    message: str
    path: str
    severity: ComparisonDiagnosticSeverity


@dataclass(frozen=True, slots=True)
class ComparisonExtensionSelector:
    """Selector declaration exposed by a comparison extension."""

    name: str
    description: str


@dataclass(frozen=True, slots=True)
class ComparisonExtensionComparator:
    """Comparator declaration exposed by a comparison extension."""

    declaration: str
    description: str


@dataclass(frozen=True, slots=True)
class ComparisonExtensionDescriptor:
    """Describes selectors, comparators, and metadata keys exposed by an extension."""

    id: str
    display_name: str
    selectors: tuple[ComparisonExtensionSelector, ...] = ()
    comparators: tuple[ComparisonExtensionComparator, ...] = ()
    metadata_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ComparisonExtensionMetadata:
    """Serializable metadata emitted by a comparison extension."""

    extension_id: str
    key: str
    value: str


@dataclass(frozen=True, slots=True)
class CohortEvidenceMetadata:
    """Parsed evidence for one cohort-aligned segment."""

    segment_index: int
    rule: str
    required_count: int
    active_count: int
    is_active: bool
    active_sources: tuple[str, ...]
    raw_value: str


class ComparisonExtensionBuilder:
    """Builds comparison extension descriptors."""

    def __init__(self, extension_id: str, display_name: str) -> None:
        if not extension_id or not extension_id.strip():
            msg = "Extension id cannot be empty."
            raise ValueError(msg)
        if not display_name or not display_name.strip():
            msg = "Extension display name cannot be empty."
            raise ValueError(msg)
        self._id = extension_id
        self._display_name = display_name
        self._selectors: list[ComparisonExtensionSelector] = []
        self._comparators: list[ComparisonExtensionComparator] = []
        self._metadata_keys: list[str] = []

    def add_selector(
        self,
        name: str,
        description: str,
    ) -> ComparisonExtensionBuilder:
        """Register a selector descriptor exposed by the extension."""

        if not name or not name.strip():
            msg = "Selector name cannot be empty."
            raise ValueError(msg)
        if not description or not description.strip():
            msg = "Selector description cannot be empty."
            raise ValueError(msg)
        self._selectors.append(ComparisonExtensionSelector(name, description))
        return self

    def add_comparator(
        self,
        declaration: str,
        description: str,
    ) -> ComparisonExtensionBuilder:
        """Register a comparator declaration exposed by the extension."""

        if not declaration or not declaration.strip():
            msg = "Comparator declaration cannot be empty."
            raise ValueError(msg)
        if not description or not description.strip():
            msg = "Comparator description cannot be empty."
            raise ValueError(msg)
        self._comparators.append(ComparisonExtensionComparator(declaration, description))
        return self

    def add_metadata_key(self, key: str) -> ComparisonExtensionBuilder:
        """Register a metadata key emitted by the extension."""

        if not key or not key.strip():
            msg = "Metadata key cannot be empty."
            raise ValueError(msg)
        self._metadata_keys.append(key)
        return self

    def build(self) -> ComparisonExtensionDescriptor:
        """Build the immutable extension descriptor."""

        return ComparisonExtensionDescriptor(
            self._id,
            self._display_name,
            tuple(self._selectors),
            tuple(self._comparators),
            tuple(self._metadata_keys),
        )


@dataclass(frozen=True, slots=True)
class ComparisonRowFinality:
    """Finality metadata for a materialized comparison row."""

    row_type: str
    row_id: str
    finality: ComparisonFinality
    reason: str
    version: int = 1
    supersedes_row_id: str | None = None


@dataclass(frozen=True, slots=True)
class ComparisonChangelogEntry:
    """One deterministic row-finality change between snapshots."""

    row_type: str
    row_id: str
    version: int
    finality: ComparisonFinality
    supersedes_row_id: str | None
    reason: str


class ComparisonChangelog:
    """Creates and replays row-finality changelogs between snapshots."""

    @staticmethod
    def create(
        previous: Iterable[ComparisonRowFinality],
        current: Iterable[ComparisonRowFinality],
    ) -> tuple[ComparisonChangelogEntry, ...]:
        """Create changelog entries from one finality snapshot to another."""

        previous_by_key = {_finality_key(row): row for row in previous}
        current_by_key = {_finality_key(row): row for row in current}
        entries: list[ComparisonChangelogEntry] = []

        for key, current_row in sorted(current_by_key.items()):
            previous_row = previous_by_key.get(key)
            if previous_row is None:
                entries.append(
                    ComparisonChangelogEntry(
                        current_row.row_type,
                        current_row.row_id,
                        current_row.version,
                        current_row.finality,
                        current_row.supersedes_row_id,
                        current_row.reason,
                    )
                )
                continue
            if (
                previous_row.finality is current_row.finality
                and previous_row.reason == current_row.reason
            ):
                continue
            entries.append(
                ComparisonChangelogEntry(
                    current_row.row_type,
                    current_row.row_id,
                    previous_row.version + 1,
                    ComparisonFinality.REVISED,
                    previous_row.row_id,
                    f"Row metadata changed from {previous_row.finality.value} "
                    f"to {current_row.finality.value}.",
                )
            )

        for key, previous_row in sorted(previous_by_key.items()):
            if key in current_by_key:
                continue
            entries.append(
                ComparisonChangelogEntry(
                    previous_row.row_type,
                    previous_row.row_id,
                    previous_row.version + 1,
                    ComparisonFinality.RETRACTED,
                    previous_row.row_id,
                    "Row was not emitted by the current snapshot.",
                )
            )
        return tuple(entries)

    @staticmethod
    def replay(
        previous: Iterable[ComparisonRowFinality],
        entries: Iterable[ComparisonChangelogEntry],
    ) -> tuple[ComparisonRowFinality, ...]:
        """Replay changelog entries over a previous finality snapshot."""

        active = {_finality_key(row): row for row in previous}
        for entry in sorted(entries, key=lambda item: (item.row_type, item.row_id, item.version)):
            key = f"{entry.row_type}\n{entry.row_id}"
            if entry.finality is ComparisonFinality.RETRACTED:
                active.pop(key, None)
                continue
            active[key] = ComparisonRowFinality(
                entry.row_type,
                entry.row_id,
                ComparisonFinality.FINAL
                if entry.finality is ComparisonFinality.REVISED
                else entry.finality,
                entry.reason,
                entry.version,
                entry.supersedes_row_id,
            )
        return tuple(sorted(active.values(), key=lambda item: (item.row_type, item.row_id)))


@dataclass(frozen=True, slots=True)
class SourceMatrixCell:
    """Directional source-pair comparison summary."""

    target_source: Any
    against_source: Any
    is_diagonal: bool
    target_has_windows: bool
    against_has_windows: bool
    overlap_row_count: int
    residual_row_count: int
    missing_row_count: int
    coverage_row_count: int
    coverage_ratio: float | None


@dataclass(frozen=True, slots=True)
class SourceMatrixResult:
    """Directional pairwise matrix over recorded window sources."""

    name: str
    window_name: str
    sources: tuple[Any, ...]
    cells: tuple[SourceMatrixCell, ...]

    def get_cell(self, target_source: Any, against_source: Any) -> SourceMatrixCell:
        """Return one directional matrix cell."""

        cell = self.try_get_cell(target_source, against_source)
        if cell is None:
            msg = "Source matrix cell was not found."
            raise KeyError(msg)
        return cell

    def try_get_cell(self, target_source: Any, against_source: Any) -> SourceMatrixCell | None:
        """Return one directional matrix cell, or ``None`` when absent."""

        for cell in self.cells:
            if cell.target_source == target_source and cell.against_source == against_source:
                return cell
        return None


@dataclass(frozen=True, slots=True)
class HierarchyComparisonRow:
    """One parent/child temporal contribution segment."""

    kind: HierarchyComparisonRowKind
    source: Any
    partition: Any
    range: TemporalRange
    parent_record_ids: tuple[WindowRecordId, ...]
    child_record_ids: tuple[WindowRecordId, ...]


@dataclass(frozen=True, slots=True)
class HierarchyComparisonResult:
    """Temporal explanation of parent and child recorded windows."""

    name: str
    parent_window_name: str
    child_window_name: str
    rows: tuple[HierarchyComparisonRow, ...]
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class LeadLagSummary:
    """Summarizes lead/lag measurements for one comparator run."""

    transition: LeadLagTransition
    axis: TemporalAxis
    tolerance_magnitude: int
    row_count: int
    target_lead_count: int
    target_lag_count: int
    equal_count: int
    missing_comparison_count: int
    outside_tolerance_count: int
    minimum_delta_magnitude: int | None
    maximum_delta_magnitude: int | None


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    """Structured result produced by a window comparison run."""

    name: str
    overlap_rows: tuple[OverlapRow, ...] = ()
    residual_rows: tuple[ResidualRow, ...] = ()
    missing_rows: tuple[MissingRow, ...] = ()
    coverage_rows: tuple[CoverageRow, ...] = ()
    gap_rows: tuple[GapRow, ...] = ()
    symmetric_difference_rows: tuple[SymmetricDifferenceRow, ...] = ()
    containment_rows: tuple[ContainmentRow, ...] = ()
    lead_lag_rows: tuple[LeadLagRow, ...] = ()
    lead_lag_summaries: tuple[LeadLagSummary, ...] = ()
    as_of_rows: tuple[AsOfRow, ...] = ()
    comparator_summaries: tuple[ComparatorSummary, ...] = ()
    coverage_summaries: tuple[CoverageSummary, ...] = ()
    diagnostics: tuple[str, ...] = ()
    evaluation_horizon: TemporalPoint | None = None
    known_at: TemporalPoint | None = None
    strict: bool = False
    extension_metadata: tuple[ComparisonExtensionMetadata, ...] = ()

    def to_json(self, path: str | Path | None = None) -> str:
        """Return a deterministic JSON representation and optionally write it."""

        text = json.dumps(_to_jsonable(self), sort_keys=True, indent=2)
        if path is not None:
            Path(path).write_text(text, encoding="utf-8")
        return text

    @property
    def row_finalities(self) -> tuple[ComparisonRowFinality, ...]:
        """Return deterministic finality metadata for emitted rows."""

        rows: list[ComparisonRowFinality] = []
        for row_type, values in _result_row_groups(self):
            for index, row in enumerate(values):
                finality = getattr(row, "finality", ComparisonFinality.FINAL)
                rows.append(
                    ComparisonRowFinality(
                        row_type,
                        f"{row_type}[{index}]",
                        finality,
                        "Depends on at least one open window clipped to the evaluation horizon."
                        if finality is ComparisonFinality.PROVISIONAL
                        else "All contributing windows were closed when the row was produced.",
                    )
                )
        return tuple(rows)

    def has_provisional_rows(self) -> bool:
        """Return whether any comparison row is provisional."""

        return any(row.finality is ComparisonFinality.PROVISIONAL for row in self.row_finalities)

    def provisional_row_finalities(self) -> tuple[ComparisonRowFinality, ...]:
        """Return row-finality metadata for provisional rows."""

        return tuple(
            row
            for row in self.row_finalities
            if row.finality is ComparisonFinality.PROVISIONAL
        )

    def final_row_finalities(self) -> tuple[ComparisonRowFinality, ...]:
        """Return row-finality metadata for final rows."""

        return tuple(
            row for row in self.row_finalities if row.finality is ComparisonFinality.FINAL
        )

    @property
    def diagnostic_rows(self) -> tuple[ComparisonDiagnostic, ...]:
        """Return structured diagnostics for this result."""

        return tuple(_diagnostic_from_code(code, strict=self.strict) for code in self.diagnostics)

    def warning_diagnostics(self) -> tuple[ComparisonDiagnostic, ...]:
        """Return diagnostics with warning severity."""

        return tuple(
            diagnostic
            for diagnostic in self.diagnostic_rows
            if diagnostic.severity is ComparisonDiagnosticSeverity.WARNING
        )

    def error_diagnostics(self) -> tuple[ComparisonDiagnostic, ...]:
        """Return diagnostics with error severity."""

        return tuple(
            diagnostic
            for diagnostic in self.diagnostic_rows
            if diagnostic.severity is ComparisonDiagnosticSeverity.ERROR
        )

    def cohort_evidence(self) -> tuple[CohortEvidenceMetadata, ...]:
        """Return parsed cohort evidence metadata emitted by this result."""

        evidence: list[CohortEvidenceMetadata] = []
        for metadata in self.extension_metadata:
            if metadata.extension_id != "spanfold.cohort":
                continue
            parsed = _parse_cohort_evidence(metadata)
            if parsed is not None:
                evidence.append(parsed)
        return tuple(evidence)

    @property
    def is_valid(self) -> bool:
        """Return whether this result has no blocking diagnostics."""

        return not self.error_diagnostics()

    def to_json_lines(self, path: str | Path | None = None) -> str:
        """Return deterministic JSON Lines, one row per comparison row."""

        rows: list[dict[str, Any]] = []
        for kind, values in (
            ("overlap", self.overlap_rows),
            ("residual", self.residual_rows),
            ("missing", self.missing_rows),
            ("coverage", self.coverage_rows),
            ("gap", self.gap_rows),
            ("symmetric_difference", self.symmetric_difference_rows),
            ("containment", self.containment_rows),
            ("lead_lag", self.lead_lag_rows),
            ("as_of", self.as_of_rows),
        ):
            for row in values:
                payload = _to_jsonable(row)
                payload["row_type"] = kind
                rows.append(payload)
        text = "\n".join(json.dumps(row, sort_keys=True) for row in rows)
        if path is not None:
            Path(path).write_text(text + ("\n" if text else ""), encoding="utf-8")
        return text

    def to_markdown(self, path: str | Path | None = None) -> str:
        """Return a compact Markdown explanation of comparison output."""

        lines = [f"# {self.name}", ""]
        if self.evaluation_horizon is not None:
            lines.append(f"evaluation horizon: {_point_label(self.evaluation_horizon)}")
        if self.known_at is not None:
            lines.append(f"knownAt={_point_label(self.known_at)}")
        if len(lines) > 2:
            lines.append("")
        lines.append("| Comparator | Rows |")
        lines.append("| --- | ---: |")
        for summary in self.comparator_summaries:
            lines.append(f"| {summary.comparator} | {summary.row_count} |")
        lines.append("")
        for label, rows in (
            ("Overlap", self.overlap_rows),
            ("Residual", self.residual_rows),
            ("Missing", self.missing_rows),
            ("Coverage", self.coverage_rows),
            ("Gap", self.gap_rows),
            ("Symmetric Difference", self.symmetric_difference_rows),
            ("Containment", self.containment_rows),
            ("Lead/Lag", self.lead_lag_rows),
            ("As-Of", self.as_of_rows),
        ):
            if not rows:
                continue
            lines.extend([f"## {label}", "", "| Window | Key | Partition | Range | Finality |"])
            lines.append("| --- | --- | --- | --- | --- |")
            for row in rows:
                lines.append(
                    f"| {row.window_name} | {row.key} | {row.partition or ''} | "
                    f"{_row_range_or_point(row)} | {row.finality.value} |"
                )
            lines.append("")
        text = "\n".join(lines).rstrip() + "\n"
        if path is not None:
            Path(path).write_text(text, encoding="utf-8")
        return text

    def to_debug_html(self, path: str | Path | None = None) -> str:
        """Return a self-contained debug HTML visualiser."""

        row_blocks: list[str] = []
        for label, color, rows in (
            ("Overlap", "#2f7d32", self.overlap_rows),
            ("Residual", "#c77800", self.residual_rows),
            ("Missing", "#b3261e", self.missing_rows),
            ("Coverage", "#2551a8", self.coverage_rows),
            ("Gap", "#6b7280", self.gap_rows),
            ("Symmetric Difference", "#8b2fc9", self.symmetric_difference_rows),
            ("Containment", "#007c89", self.containment_rows),
            ("Lead/Lag", "#8f4b00", self.lead_lag_rows),
            ("As-Of", "#6d5dfc", self.as_of_rows),
        ):
            for row in rows:
                row_blocks.append(_html_row(label, color, row))
        body = "\n".join(row_blocks) or "<p>No comparison rows.</p>"
        summary_rows = "".join(
            f"<tr><td>{html.escape(summary.comparator)}</td>"
            f"<td>{summary.row_count}</td></tr>"
            for summary in self.comparator_summaries
        )
        extension_rows = "".join(
            f"<li>{html.escape(metadata.extension_id)} "
            f"{html.escape(metadata.key)}={html.escape(metadata.value)}</li>"
            for metadata in self.extension_metadata
        )
        extension_block = (
            f"<h2>Extension metadata</h2><ul>{extension_rows}</ul>"
            if extension_rows
            else ""
        )
        text = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{html.escape(self.name)} - Spanfold debug</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #17202a; }}
.summary {{ border-collapse: collapse; margin-bottom: 1.5rem; }}
.summary th,.summary td {{
  border-bottom: 1px solid #d9dee7; padding: .35rem .6rem;
}}
.row {{
  margin: .7rem 0; display: grid; grid-template-columns: 9rem 1fr;
  gap: .75rem; align-items: center;
}}
.track {{
  position: relative; height: 1.5rem; background: #eef2f6;
  border-radius: 4px; overflow: hidden;
}}
.bar {{ position: absolute; top: 0; bottom: 0; min-width: 2px; }}
.meta {{ font-size: .9rem; }}
</style>
</head>
<body>
<h1>{html.escape(self.name)}</h1>
<table class="summary"><tr><th>Comparator</th><th>Rows</th></tr>
{summary_rows}
</table>
{extension_block}
{body}
</body>
</html>
"""
        if path is not None:
            Path(path).write_text(text, encoding="utf-8")
        return text

    def explain(self, *, markdown: bool = True, path: str | Path | None = None) -> str:
        """Return deterministic human-readable comparison output."""

        prefix = "# " if markdown else ""
        lines = [f"{prefix}Comparison Explain: {self.name}", ""]
        if self.diagnostic_rows:
            lines.append("## Diagnostics" if markdown else "Diagnostics")
            for index, diagnostic in enumerate(self.diagnostic_rows):
                lines.append(
                    f"- diagnostic[{index}]: {diagnostic.severity.value} "
                    f"{diagnostic.code} path={diagnostic.path}"
                )
            lines.append("")
        if self.extension_metadata:
            lines.append("## Extension Metadata" if markdown else "Extension Metadata")
            for index, metadata in enumerate(self.extension_metadata):
                lines.append(
                    f"- extensionMetadata[{index}]: "
                    f"{metadata.extension_id}.{metadata.key}={metadata.value}"
                )
            lines.append("")
        if self.comparator_summaries:
            lines.append("## Summaries" if markdown else "Summaries")
            for summary in self.comparator_summaries:
                lines.append(f"- comparator: {summary.comparator}; rows={summary.row_count}")
            lines.append("")
        for row_type, values in _result_row_groups(self):
            for index, row in enumerate(values):
                record_ids = _row_record_ids(row)
                lines.append(
                    f"- {row_type}[{index}]: {_row_range_or_point(row)}; "
                    f"records={','.join(str(record_id) for record_id in record_ids)}"
                )
        text = "\n".join(lines).rstrip() + "\n"
        if path is not None:
            Path(path).write_text(text, encoding="utf-8")
        return text


@dataclass(frozen=True, slots=True)
class _NormalizedWindow:
    window: WindowRecord
    source_label: str
    range: TemporalRange
    finality: ComparisonFinality
    record_ids: tuple[WindowRecordId, ...] = ()


@dataclass(frozen=True, slots=True)
class _CohortDefinition:
    name: str
    sources: tuple[Any, ...]
    activity: CohortActivity


class _Any:
    pass


_ANY = _Any()


class WindowComparisonBuilder:
    """Staged comparison builder for recorded window history."""

    def __init__(self, history: WindowHistory, name: str) -> None:
        if not name or not name.strip():
            msg = "Comparison name cannot be empty."
            raise ValueError(msg)
        self._history = history
        self._name = name
        self._target: Callable[[WindowRecord], bool] | None = None
        self._against: list[Callable[[WindowRecord], bool]] = []
        self._cohorts: list[_CohortDefinition] = []
        self._window_name: str | None = None
        self._key: Any = _ANY
        self._partition: Any = _ANY
        self._segments: dict[str, Any] = {}
        self._tags: dict[str, Any] = {}
        self._comparators: tuple[str, ...] = ("overlap", "residual", "missing")
        self._axis = TemporalAxis.PROCESSING_POSITION
        self._horizon: TemporalPoint | None = None
        self._known_at: TemporalPoint | None = None
        self._missing_event_time_policy = "reject"
        self._coalesce_adjacent = False
        self._duplicate_windows = "preserve"
        self._strict = False
        self._extension_metadata: list[ComparisonExtensionMetadata] = []

    def target(
        self,
        source: Any | None = None,
        *,
        predicate: Callable[[WindowRecord], bool] | None = None,
    ) -> WindowComparisonBuilder:
        """Select the target side by source or custom predicate."""

        self._target = (
            predicate if predicate is not None else lambda window: window.source == source
        )
        return self

    def against(
        self,
        source: Any | None = None,
        *,
        predicate: Callable[[WindowRecord], bool] | None = None,
    ) -> WindowComparisonBuilder:
        """Add a comparison side by source or custom predicate."""

        self._against.append(
            predicate if predicate is not None else lambda window: window.source == source
        )
        return self

    def against_cohort(
        self,
        name: str,
        *,
        sources: Iterable[Any],
        activity: CohortActivity | None = None,
    ) -> WindowComparisonBuilder:
        """Add a derived comparison lane backed by a source cohort."""

        cohort_sources = tuple(sources)
        if not cohort_sources:
            msg = "Cohort must declare at least one source."
            raise ValueError(msg)
        cohort_activity = activity or CohortActivity.any()
        if (
            cohort_activity.count is not None
            and cohort_activity.count > len(cohort_sources)
        ):
            msg = "Cohort activity count cannot exceed the number of declared sources."
            raise ValueError(msg)
        self._cohorts.append(_CohortDefinition(name, cohort_sources, cohort_activity))
        return self

    def within(
        self,
        *,
        window_name: str | None = None,
        key: Any = _ANY,
        partition: Any = _ANY,
        segments: Mapping[str, Any] | None = None,
        tags: Mapping[str, Any] | None = None,
    ) -> WindowComparisonBuilder:
        """Limit comparison scope by window, key, or partition."""

        self._window_name = window_name
        self._key = key
        self._partition = partition
        self._segments = dict(segments or {})
        self._tags = dict(tags or {})
        return self

    def normalize(
        self,
        policy: ComparisonNormalizationPolicy | None = None,
        *,
        axis: TemporalAxis = TemporalAxis.PROCESSING_POSITION,
        horizon: TemporalPoint | None = None,
        known_at: TemporalPoint | None = None,
        missing_event_time: str = "reject",
        coalesce_adjacent: bool = False,
        duplicate_windows: str = "preserve",
    ) -> WindowComparisonBuilder:
        """Choose the comparison axis, open-window horizon, and availability point."""

        if policy is not None:
            self._apply_normalization_policy(policy)
            return self
        if missing_event_time not in {"reject", "exclude"}:
            msg = "missing_event_time must be 'reject' or 'exclude'."
            raise ValueError(msg)
        if duplicate_windows not in {"preserve", "reject"}:
            msg = "duplicate_windows must be 'preserve' or 'reject'."
            raise ValueError(msg)
        self._axis = axis
        self._horizon = horizon
        self._known_at = known_at
        self._missing_event_time_policy = missing_event_time
        self._coalesce_adjacent = coalesce_adjacent
        self._duplicate_windows = duplicate_windows
        return self

    def _apply_normalization_policy(
        self,
        policy: ComparisonNormalizationPolicy,
    ) -> None:
        self._axis = policy.time_axis
        self._horizon = policy.open_window_horizon
        self._known_at = policy.known_at
        self._missing_event_time_policy = policy.null_timestamp_policy.value
        self._coalesce_adjacent = policy.coalesce_adjacent_windows
        self._duplicate_windows = policy.duplicate_window_policy.value

    def using(self, *comparators: str) -> WindowComparisonBuilder:
        """Choose comparators to run by name."""

        self._comparators = (
            tuple(_normalize_comparator(comparator) for comparator in comparators)
            or self._comparators
        )
        return self

    def strict(self) -> WindowComparisonBuilder:
        """Promote warning diagnostics to blocking errors for this comparison."""

        self._strict = True
        return self

    def run(self) -> ComparisonResult:
        """Run the configured comparison."""

        if self._target is None:
            msg = "Comparison target selector is required."
            raise ValueError(msg)
        if not self._against and not self._cohorts:
            msg = "At least one against selector is required."
            raise ValueError(msg)

        diagnostics = self._normalization_diagnostics()
        target_windows = self._select(self._target, "target", diagnostics)
        against_windows = [
            item
            for index, selector in enumerate(self._against)
            for item in self._select(selector, f"against-{index + 1}", diagnostics)
        ]
        self._extension_metadata = []
        for cohort in self._cohorts:
            cohort_windows, metadata = self._select_cohort(cohort, target_windows, diagnostics)
            against_windows.extend(cohort_windows)
            self._extension_metadata.extend(metadata)
        target_windows = self._postprocess_normalized(target_windows, diagnostics)
        against_windows = self._postprocess_normalized(against_windows, diagnostics)
        return _run_comparison(
            self._name,
            target_windows,
            against_windows,
            self._comparators,
            evaluation_horizon=self._horizon,
            known_at=self._known_at,
            diagnostics=diagnostics,
            strict=self._strict,
            extension_metadata=tuple(self._extension_metadata),
        )

    def run_live(self, horizon: TemporalPoint) -> ComparisonResult:
        """Run the comparison with open windows clipped to an evaluation horizon."""

        return self.normalize(axis=horizon.axis, horizon=horizon).run()

    def _select(
        self,
        selector: Callable[[WindowRecord], bool],
        label: str,
        diagnostics: list[str],
    ) -> list[_NormalizedWindow]:
        rows: list[_NormalizedWindow] = []
        for window in self._history.windows:
            if self._window_name is not None and window.window_name != self._window_name:
                continue
            if self._key is not _ANY and window.key != self._key:
                continue
            if self._partition is not _ANY and window.partition != self._partition:
                continue
            if not _has_segments(window, self._segments):
                continue
            if not _has_tags(window, self._tags):
                continue
            if not selector(window):
                continue
            if not self._is_known_at(window):
                _append_once(diagnostics, "future_window_excluded")
                continue
            try:
                temporal_range = window.range_for_axis(self._axis, horizon=self._horizon)
            except ValueError:
                _append_once(diagnostics, "invalid_range_duration")
                continue
            if temporal_range is None:
                self._diagnose_unusable_window(window, diagnostics)
                continue
            finality = (
                ComparisonFinality.FINAL
                if temporal_range.is_closed
                else ComparisonFinality.PROVISIONAL
            )
            rows.append(_NormalizedWindow(window, label, temporal_range, finality))
        return rows

    def _select_cohort(
        self,
        cohort: _CohortDefinition,
        targets: list[_NormalizedWindow],
        diagnostics: list[str],
    ) -> tuple[list[_NormalizedWindow], tuple[ComparisonExtensionMetadata, ...]]:
        member_windows = self._select(
            lambda window: window.source in cohort.sources,
            cohort.name,
            diagnostics,
        )
        return _materialize_cohort(cohort, member_windows, targets)

    def _normalization_diagnostics(self) -> list[str]:
        diagnostics: list[str] = []
        if (
            self._known_at is not None
            and self._known_at.axis is not TemporalAxis.PROCESSING_POSITION
        ):
            diagnostics.append("known_at_requires_processing_position")
        if self._known_at is None and any(
            _try_parse_as_of(comparator) is not None for comparator in self._comparators
        ):
            diagnostics.append("future_leakage_risk")
        return diagnostics

    def _is_known_at(self, window: WindowRecord) -> bool:
        if self._known_at is None:
            return True
        if self._known_at.axis is not TemporalAxis.PROCESSING_POSITION:
            return True
        available_position = (
            window.end_position if window.end_position is not None else window.start_position
        )
        return available_position <= self._known_at.position

    def _diagnose_unusable_window(
        self,
        window: WindowRecord,
        diagnostics: list[str],
    ) -> None:
        if self._axis is TemporalAxis.TIMESTAMP and _missing_event_time(window):
            if self._missing_event_time_policy == "reject":
                _append_once(diagnostics, "missing_event_time")
            return
        if not window.is_closed and self._horizon is None:
            _append_once(diagnostics, "open_windows_without_policy")

    def _postprocess_normalized(
        self,
        windows: list[_NormalizedWindow],
        diagnostics: list[str],
    ) -> list[_NormalizedWindow]:
        deduplicated = _deduplicate_normalized(
            windows,
            diagnostics,
            reject=self._duplicate_windows == "reject",
        )
        if self._coalesce_adjacent:
            return _coalesce_normalized(deduplicated)
        return deduplicated


def _run_comparison(
    name: str,
    targets: list[_NormalizedWindow],
    against: list[_NormalizedWindow],
    comparators: tuple[str, ...],
    *,
    evaluation_horizon: TemporalPoint | None = None,
    known_at: TemporalPoint | None = None,
    diagnostics: Iterable[str] = (),
    strict: bool = False,
    extension_metadata: tuple[ComparisonExtensionMetadata, ...] = (),
) -> ComparisonResult:
    target_groups = _group(targets)
    against_groups = _group(against)
    scopes = sorted(
        set(target_groups) | set(against_groups), key=lambda item: tuple(map(repr, item))
    )

    overlap_rows: list[OverlapRow] = []
    residual_rows: list[ResidualRow] = []
    missing_rows: list[MissingRow] = []
    coverage_rows: list[CoverageRow] = []
    gap_rows: list[GapRow] = []
    symmetric_difference_rows: list[SymmetricDifferenceRow] = []
    containment_rows: list[ContainmentRow] = []
    lead_lag_rows: list[LeadLagRow] = []
    lead_lag_summaries: list[LeadLagSummary] = []
    as_of_rows: list[AsOfRow] = []
    lead_lag_options: list[tuple[str, _LeadLagOptions]] = []
    as_of_options: list[tuple[str, _AsOfOptions]] = []
    for comparator in comparators:
        lead_lag_option = _try_parse_lead_lag(comparator)
        if lead_lag_option is not None:
            lead_lag_options.append((comparator, lead_lag_option))
        as_of_option = _try_parse_as_of(comparator)
        if as_of_option is not None:
            as_of_options.append((comparator, as_of_option))

    for scope in scopes:
        target_items = sorted(target_groups.get(scope, ()), key=lambda item: item.range.start)
        against_items = sorted(against_groups.get(scope, ()), key=lambda item: item.range.start)
        window_name, key, partition = scope

        if "overlap" in comparators or "coverage" in comparators:
            for target in target_items:
                overlapping_against: list[_NormalizedWindow] = []
                for candidate in against_items:
                    intersection = target.range.intersection(candidate.range)
                    if intersection is None:
                        continue
                    overlapping_against.append(candidate)
                    if "overlap" in comparators:
                        overlap_rows.append(
                            OverlapRow(
                                window_name,
                                key,
                                partition,
                                intersection,
                                _record_ids(target),
                                _record_ids(candidate),
                                _finality(target, candidate),
                            )
                        )
                if "coverage" in comparators:
                    coverage_rows.append(
                        _coverage_row(window_name, key, partition, target, overlapping_against)
                    )

        if "residual" in comparators:
            against_ranges = [item.range for item in against_items]
            for target in target_items:
                for temporal_range in target.range.residual(against_ranges):
                    residual_rows.append(
                        ResidualRow(
                            window_name,
                            key,
                            partition,
                            temporal_range,
                            _record_ids(target),
                            target.finality,
                        )
                    )

        if "missing" in comparators:
            target_ranges = [item.range for item in target_items]
            for item in against_items:
                for temporal_range in item.range.residual(target_ranges):
                    missing_rows.append(
                        MissingRow(
                            window_name,
                            key,
                            partition,
                            temporal_range,
                            _record_ids(item),
                            item.finality,
                        )
                    )

        if "gap" in comparators:
            gap_rows.extend(_gap_rows(window_name, key, partition, target_items, against_items))

        if "symmetric_difference" in comparators:
            symmetric_difference_rows.extend(
                _symmetric_difference_rows(
                    window_name,
                    key,
                    partition,
                    target_items,
                    against_items,
                )
            )

        if "containment" in comparators:
            containment_rows.extend(
                _containment_rows(window_name, key, partition, target_items, against_items)
            )

    lead_lag_counts: dict[str, int] = {}
    for comparator, lead_lag_option in lead_lag_options:
        before = len(lead_lag_rows)
        for scope in scopes:
            window_name, key, partition = scope
            lead_lag_rows.extend(
                _lead_lag_rows(
                    window_name,
                    key,
                    partition,
                    target_groups.get(scope, ()),
                    against_groups.get(scope, ()),
                    lead_lag_option,
                )
            )
        lead_lag_counts[comparator] = len(lead_lag_rows) - before
        lead_lag_summaries.append(
            _lead_lag_summary(
                lead_lag_option,
                lead_lag_rows[before:],
                lead_lag_counts[comparator],
            )
        )

    as_of_counts: dict[str, int] = {}
    result_diagnostics = list(diagnostics)
    for comparator, as_of_option in as_of_options:
        before = len(as_of_rows)
        for scope in scopes:
            window_name, key, partition = scope
            as_of_rows.extend(
                _as_of_rows(
                    window_name,
                    key,
                    partition,
                    target_groups.get(scope, ()),
                    against_groups.get(scope, ()),
                    as_of_option,
                    result_diagnostics,
                )
            )
        as_of_counts[comparator] = len(as_of_rows) - before

    summaries = tuple(
        ComparatorSummary(
            comparator,
            _count_for(
                comparator,
                overlap_rows,
                residual_rows,
                missing_rows,
                coverage_rows,
                gap_rows,
                symmetric_difference_rows,
                containment_rows,
                lead_lag_rows,
                lead_lag_counts,
                as_of_rows,
                as_of_counts,
            ),
        )
        for comparator in comparators
    )
    coverage_summaries = _coverage_summaries(coverage_rows)
    return ComparisonResult(
        name=name,
        overlap_rows=tuple(overlap_rows),
        residual_rows=tuple(residual_rows),
        missing_rows=tuple(missing_rows),
        coverage_rows=tuple(coverage_rows),
        gap_rows=tuple(gap_rows),
        symmetric_difference_rows=tuple(symmetric_difference_rows),
        containment_rows=tuple(containment_rows),
        lead_lag_rows=tuple(lead_lag_rows),
        lead_lag_summaries=tuple(lead_lag_summaries),
        as_of_rows=tuple(as_of_rows),
        comparator_summaries=summaries,
        coverage_summaries=coverage_summaries,
        diagnostics=tuple(result_diagnostics),
        evaluation_horizon=evaluation_horizon,
        known_at=known_at,
        strict=strict,
        extension_metadata=extension_metadata,
    )


def _materialize_cohort(
    cohort: _CohortDefinition,
    members: list[_NormalizedWindow],
    targets: list[_NormalizedWindow],
) -> tuple[list[_NormalizedWindow], tuple[ComparisonExtensionMetadata, ...]]:
    grouped = _group(members)
    target_groups = _group(targets)
    materialized: list[_NormalizedWindow] = []
    metadata: list[ComparisonExtensionMetadata] = []
    segment_index = 0
    for scope in sorted(set(grouped) | set(target_groups), key=lambda item: tuple(map(repr, item))):
        window_name, key, partition = scope
        items = grouped.get(scope, [])
        scope_targets = target_groups.get(scope, [])
        boundaries = sorted(
            {
                point
                for item in [*items, *scope_targets]
                for point in (item.range.start, item.range.require_end())
            }
        )
        for start, end in zip(boundaries, boundaries[1:], strict=False):
            if start >= end:
                continue
            active_items = [
                item
                for item in items
                if item.range.start <= start and end <= item.range.require_end()
            ]
            active_sources = {item.window.source for item in active_items}
            is_active = cohort.activity.is_active(len(active_sources), len(cohort.sources))
            if active_items or any(
                item.range.start <= start and end <= item.range.require_end()
                for item in scope_targets
            ):
                metadata.append(
                    ComparisonExtensionMetadata(
                        "spanfold.cohort",
                        f"segment[{segment_index}]",
                        _cohort_evidence_value(cohort, active_sources, is_active),
                    )
                )
                segment_index += 1
            if not is_active:
                continue
            record = ClosedWindow(
                window_name,
                key,
                start.position,
                end.position,
                source=cohort.name,
                partition=partition,
            )
            materialized.append(
                _NormalizedWindow(
                    record,
                    cohort.name,
                    TemporalRange.closed(start, end),
                    ComparisonFinality.PROVISIONAL
                    if any(
                        item.finality is ComparisonFinality.PROVISIONAL
                        for item in active_items
                    )
                    else ComparisonFinality.FINAL,
                    tuple(item.window.id for item in active_items),
                )
            )
    return materialized, tuple(metadata)


def _cohort_evidence_value(
    cohort: _CohortDefinition,
    active_sources: Iterable[Any],
    is_active: bool,
) -> str:
    ordered_sources = sorted((str(source) for source in active_sources), key=repr)
    return (
        f"rule={cohort.activity.name.replace('_', '-')}; "
        f"required={cohort.activity.required_count(len(cohort.sources))}; "
        f"activeCount={len(ordered_sources)}; "
        f"isActive={'true' if is_active else 'false'}; "
        f"activeSources={','.join(ordered_sources)}"
    )


def build_source_matrix(
    history: WindowHistory,
    name: str,
    window_name: str,
    sources: Iterable[Any],
) -> SourceMatrixResult:
    """Build a directional pairwise source matrix for one recorded window."""

    ordered_sources = tuple(sources)
    if not ordered_sources:
        msg = "At least one source is required."
        raise ValueError(msg)

    source_has_windows = {
        source: any(
            window.window_name == window_name and window.source == source
            for window in history.windows
        )
        for source in ordered_sources
    }
    cells: list[SourceMatrixCell] = []

    for target_source in ordered_sources:
        for against_source in ordered_sources:
            target_has_windows = source_has_windows[target_source]
            against_has_windows = source_has_windows[against_source]
            if target_source == against_source:
                cells.append(
                    SourceMatrixCell(
                        target_source,
                        against_source,
                        True,
                        target_has_windows,
                        against_has_windows,
                        0,
                        0,
                        0,
                        0,
                        1.0 if target_has_windows else None,
                    )
                )
                continue

            result = (
                history.compare(f"{name} {target_source} vs {against_source}")
                .target(target_source)
                .against(against_source)
                .within(window_name=window_name)
                .using("overlap", "residual", "missing", "coverage")
                .run()
            )
            cells.append(
                SourceMatrixCell(
                    target_source,
                    against_source,
                    False,
                    target_has_windows,
                    against_has_windows,
                    len(result.overlap_rows),
                    len(result.residual_rows),
                    len(result.missing_rows),
                    len(result.coverage_rows),
                    _aggregate_coverage_ratio(result.coverage_rows),
                )
            )

    return SourceMatrixResult(name, window_name, ordered_sources, tuple(cells))


def build_hierarchy_comparison(
    history: WindowHistory,
    name: str,
    parent_window_name: str,
    child_window_name: str,
) -> HierarchyComparisonResult:
    """Compare parent windows against child contribution windows."""

    parents = [window for window in history.windows if window.window_name == parent_window_name]
    children = [window for window in history.windows if window.window_name == child_window_name]
    diagnostics: list[str] = []
    if not parents:
        diagnostics.append("missing_parent_lineage")
    if not children:
        diagnostics.append("missing_child_lineage")

    rows: list[HierarchyComparisonRow] = []
    scopes = sorted(
        {
            (window.source, window.partition)
            for window in [*parents, *children]
        },
        key=lambda item: (repr(item[0]), repr(item[1])),
    )
    for source, partition in scopes:
        rows.extend(_hierarchy_rows_for_scope(source, partition, parents, children))

    return HierarchyComparisonResult(
        name,
        parent_window_name,
        child_window_name,
        tuple(rows),
        tuple(diagnostics),
    )


def total_position_length(rows: Iterable[Any]) -> int:
    """Return total processing-position length across rows with ranges."""

    return sum(row.range.position_length() for row in rows)


def total_time_duration(rows: Iterable[Any]) -> timedelta:
    """Return total event-time duration across rows with ranges."""

    total = timedelta()
    for row in rows:
        total += row.range.time_duration()
    return total


def total_target_magnitude(rows: Iterable[CoverageRow]) -> float:
    """Return total target denominator magnitude across coverage rows."""

    return sum(row.target_magnitude for row in rows)


def total_covered_magnitude(rows: Iterable[CoverageRow]) -> float:
    """Return total comparison-covered numerator magnitude across coverage rows."""

    return sum(row.covered_magnitude for row in rows)


def _hierarchy_rows_for_scope(
    source: Any,
    partition: Any,
    parents: list[WindowRecord],
    children: list[WindowRecord],
) -> list[HierarchyComparisonRow]:
    scoped_parents = [
        window for window in parents if window.source == source and window.partition == partition
    ]
    scoped_children = [
        window for window in children if window.source == source and window.partition == partition
    ]
    boundaries = sorted(
        {
            point
            for window in [*scoped_parents, *scoped_children]
            if window.end_position is not None
            for point in (
                TemporalPoint.for_position(window.start_position),
                TemporalPoint.for_position(window.end_position),
            )
        }
    )
    rows: list[HierarchyComparisonRow] = []
    for start, end in zip(boundaries, boundaries[1:], strict=False):
        if start >= end:
            continue
        parent_ids = _active_hierarchy_ids(scoped_parents, start, end)
        child_ids = _active_hierarchy_ids(scoped_children, start, end)
        if not parent_ids and not child_ids:
            continue
        rows.append(
            HierarchyComparisonRow(
                _hierarchy_kind(len(parent_ids), len(child_ids)),
                source,
                partition,
                TemporalRange.closed(start, end),
                parent_ids,
                child_ids,
            )
        )
    return rows


def _active_hierarchy_ids(
    windows: list[WindowRecord],
    start: TemporalPoint,
    end: TemporalPoint,
) -> tuple[WindowRecordId, ...]:
    ids: list[WindowRecordId] = []
    for window in windows:
        temporal_range = window.range_for_axis()
        if temporal_range is None:
            continue
        if temporal_range.start <= start and end <= temporal_range.require_end():
            ids.append(window.id)
    return tuple(sorted(ids, key=lambda item: item.value))


def _hierarchy_kind(parent_count: int, child_count: int) -> HierarchyComparisonRowKind:
    if parent_count > 0 and child_count > 0:
        return HierarchyComparisonRowKind.PARENT_EXPLAINED
    if parent_count > 0:
        return HierarchyComparisonRowKind.UNEXPLAINED_PARENT
    return HierarchyComparisonRowKind.ORPHAN_CHILD


def _group(
    items: Iterable[_NormalizedWindow],
) -> dict[tuple[str, Any, Any], list[_NormalizedWindow]]:
    grouped: dict[tuple[str, Any, Any], list[_NormalizedWindow]] = defaultdict(list)
    for item in items:
        grouped[(item.window.window_name, item.window.key, item.window.partition)].append(item)
    return grouped


def _deduplicate_normalized(
    windows: list[_NormalizedWindow],
    diagnostics: list[str],
    *,
    reject: bool,
) -> list[_NormalizedWindow]:
    seen: set[tuple[str, str, str, str, str, TemporalPoint, TemporalPoint | None, str]] = set()
    rows: list[_NormalizedWindow] = []
    for window in windows:
        key = (
            window.source_label,
            window.window.window_name,
            repr(window.window.key),
            repr(window.window.source),
            repr(window.window.partition),
            window.range.start,
            window.range.end,
            window.range.end_status.value,
        )
        if key in seen:
            _append_once(diagnostics, "duplicate_window")
            if reject:
                continue
        else:
            seen.add(key)
        rows.append(window)
    return rows


def _coalesce_normalized(windows: list[_NormalizedWindow]) -> list[_NormalizedWindow]:
    groups: dict[
        tuple[str, str, str, str, str, str, str],
        list[_NormalizedWindow],
    ] = defaultdict(list)
    for window in windows:
        key = (
            window.source_label,
            window.window.window_name,
            repr(window.window.key),
            repr(window.window.source),
            repr(window.window.partition),
            repr(window.window.segments),
            repr(window.window.tags),
        )
        groups[key].append(window)

    rows: list[_NormalizedWindow] = []
    for _, items in sorted(groups.items(), key=lambda item: item[0]):
        ordered = sorted(items, key=lambda item: item.range.start)
        for item in ordered:
            if not rows or not _can_coalesce(rows[-1], item):
                rows.append(item)
                continue
            previous = rows[-1]
            rows[-1] = _NormalizedWindow(
                previous.window,
                previous.source_label,
                TemporalRange(
                    previous.range.start,
                    item.range.end,
                    item.range.end_status,
                ),
                _finality(previous, item),
                (*_record_ids(previous), *_record_ids(item)),
            )
    return rows


def _can_coalesce(first: _NormalizedWindow, second: _NormalizedWindow) -> bool:
    return (
        first.source_label == second.source_label
        and first.window.window_name == second.window.window_name
        and first.window.key == second.window.key
        and first.window.source == second.window.source
        and first.window.partition == second.window.partition
        and first.window.segments == second.window.segments
        and first.window.tags == second.window.tags
        and first.range.end == second.range.start
    )


def _record_ids(window: _NormalizedWindow) -> tuple[WindowRecordId, ...]:
    return window.record_ids or (window.window.id,)


def _primary_record_id(window: _NormalizedWindow) -> WindowRecordId:
    return _record_ids(window)[0]


def _has_segments(window: WindowRecord, required: Mapping[str, Any]) -> bool:
    return all(
        any(segment.name == name and segment.value == value for segment in window.segments)
        for name, value in required.items()
    )


def _has_tags(window: WindowRecord, required: Mapping[str, Any]) -> bool:
    return all(
        any(tag.name == name and tag.value == value for tag in window.tags)
        for name, value in required.items()
    )


def _coverage_row(
    window_name: str,
    key: Any,
    partition: Any,
    target: _NormalizedWindow,
    against: list[_NormalizedWindow],
) -> CoverageRow:
    intersections = [
        intersection
        for item in against
        if (intersection := target.range.intersection(item.range)) is not None
    ]
    covered = sum(_merged_ranges(intersections), start=0.0)
    return CoverageRow(
        window_name,
        key,
        partition,
        target.range,
        target.range.magnitude(),
        covered,
        _record_ids(target),
        tuple(record_id for item in against for record_id in _record_ids(item)),
        ComparisonFinality.PROVISIONAL
        if target.finality is ComparisonFinality.PROVISIONAL
        or any(item.finality is ComparisonFinality.PROVISIONAL for item in against)
        else ComparisonFinality.FINAL,
    )


def _aggregate_coverage_ratio(rows: tuple[CoverageRow, ...]) -> float | None:
    target = sum(row.target_magnitude for row in rows)
    if target == 0:
        return None
    covered = sum(row.covered_magnitude for row in rows)
    return covered / target


def _coverage_summaries(rows: list[CoverageRow]) -> tuple[CoverageSummary, ...]:
    groups: dict[tuple[str, Any, Any], list[CoverageRow]] = defaultdict(list)
    for row in rows:
        groups[(row.window_name, row.key, row.partition)].append(row)

    summaries: list[CoverageSummary] = []
    for scope, items in sorted(groups.items(), key=lambda item: tuple(map(repr, item[0]))):
        target = total_target_magnitude(items)
        covered = total_covered_magnitude(items)
        summaries.append(
            CoverageSummary(
                scope[0],
                scope[1],
                scope[2],
                target,
                covered,
                0.0 if target == 0 else covered / target,
            )
        )
    return tuple(summaries)


def _gap_rows(
    window_name: str,
    key: Any,
    partition: Any,
    targets: list[_NormalizedWindow],
    against: list[_NormalizedWindow],
) -> list[GapRow]:
    merged = _merge_temporal_ranges([item.range for item in [*targets, *against]])
    rows: list[GapRow] = []
    for previous, current in zip(merged, merged[1:], strict=False):
        previous_end = previous.require_end()
        if previous_end < current.start:
            rows.append(
                GapRow(
                    window_name,
                    key,
                    partition,
                    TemporalRange.closed(previous_end, current.start),
                )
            )
    return rows


def _symmetric_difference_rows(
    window_name: str,
    key: Any,
    partition: Any,
    targets: list[_NormalizedWindow],
    against: list[_NormalizedWindow],
) -> list[SymmetricDifferenceRow]:
    rows: list[SymmetricDifferenceRow] = []
    against_ranges = [item.range for item in against]
    target_ranges = [item.range for item in targets]

    for target in targets:
        for temporal_range in target.range.residual(against_ranges):
            rows.append(
                SymmetricDifferenceRow(
                    window_name,
                    key,
                    partition,
                    temporal_range,
                    ComparisonSide.TARGET,
                    _record_ids(target),
                    (),
                    target.finality,
                )
            )

    for item in against:
        for temporal_range in item.range.residual(target_ranges):
            rows.append(
                SymmetricDifferenceRow(
                    window_name,
                    key,
                    partition,
                    temporal_range,
                    ComparisonSide.AGAINST,
                    (),
                    _record_ids(item),
                    item.finality,
                )
            )

    return sorted(rows, key=_row_sort_key)


def _containment_rows(
    window_name: str,
    key: Any,
    partition: Any,
    targets: list[_NormalizedWindow],
    containers: list[_NormalizedWindow],
) -> list[ContainmentRow]:
    rows: list[ContainmentRow] = []
    container_ranges = [container.range for container in containers]

    for target in targets:
        contained: list[ContainmentRow] = []
        for container in containers:
            intersection = target.range.intersection(container.range)
            if intersection is None:
                continue
            contained.append(
                ContainmentRow(
                    window_name,
                    key,
                    partition,
                    intersection,
                    ContainmentStatus.CONTAINED,
                    _record_ids(target),
                    _record_ids(container),
                    _finality(target, container),
                )
            )

        residual_ranges = target.range.residual(container_ranges)
        if not contained and not residual_ranges:
            continue
        if not contained:
            rows.append(
                ContainmentRow(
                    window_name,
                    key,
                    partition,
                    target.range,
                    ContainmentStatus.NOT_CONTAINED,
                    _record_ids(target),
                    (),
                    target.finality,
                )
            )
            continue

        rows.extend(contained)
        first_contained_start = min(row.range.start for row in contained)
        last_contained_end = max(row.range.require_end() for row in contained)
        for temporal_range in residual_ranges:
            status = ContainmentStatus.NOT_CONTAINED
            if temporal_range.require_end() <= first_contained_start:
                status = ContainmentStatus.LEFT_OVERHANG
            elif temporal_range.start >= last_contained_end:
                status = ContainmentStatus.RIGHT_OVERHANG
            rows.append(
                ContainmentRow(
                    window_name,
                    key,
                    partition,
                    temporal_range,
                    status,
                    _record_ids(target),
                    (),
                    target.finality,
                )
            )

    return sorted(rows, key=_row_sort_key)


@dataclass(frozen=True, slots=True)
class _LeadLagOptions:
    transition: LeadLagTransition
    axis: TemporalAxis
    tolerance_magnitude: int


@dataclass(frozen=True, slots=True)
class _AsOfOptions:
    direction: AsOfDirection
    axis: TemporalAxis
    tolerance_magnitude: int


@dataclass(frozen=True, slots=True)
class _TransitionPoint:
    record_id: WindowRecordId
    point: TemporalPoint
    finality: ComparisonFinality


def _lead_lag_rows(
    window_name: str,
    key: Any,
    partition: Any,
    targets: Iterable[_NormalizedWindow],
    against: Iterable[_NormalizedWindow],
    options: _LeadLagOptions,
) -> list[LeadLagRow]:
    candidates = sorted(
        (
            _TransitionPoint(_primary_record_id(item), point, item.finality)
            for item in against
            if item.range.axis is options.axis
            if (point := _transition_point(item.range, options.transition)) is not None
        ),
        key=lambda item: item.point,
    )
    rows: list[LeadLagRow] = []

    for target in targets:
        target_point = _transition_point(target.range, options.transition)
        if target.range.axis is not options.axis or target_point is None:
            continue
        if not candidates:
            rows.append(
                LeadLagRow(
                    window_name,
                    key,
                    partition,
                    options.transition,
                    options.axis,
                    target_point,
                    None,
                    None,
                    options.tolerance_magnitude,
                    False,
                    LeadLagDirection.MISSING_COMPARISON,
                    _primary_record_id(target),
                    None,
                    target.finality,
                )
            )
            continue

        nearest = _nearest_transition(candidates, target_point, options.axis)
        delta = _delta_magnitude(target_point, nearest.point, options.axis)
        absolute_delta = abs(delta)
        rows.append(
            LeadLagRow(
                window_name,
                key,
                partition,
                options.transition,
                options.axis,
                target_point,
                nearest.point,
                delta,
                options.tolerance_magnitude,
                absolute_delta <= options.tolerance_magnitude,
                _lead_lag_direction(delta),
                _primary_record_id(target),
                nearest.record_id,
                ComparisonFinality.PROVISIONAL
                if target.finality is ComparisonFinality.PROVISIONAL
                or nearest.finality is ComparisonFinality.PROVISIONAL
                else ComparisonFinality.FINAL,
            )
        )
    return rows


def _lead_lag_summary(
    options: _LeadLagOptions,
    rows: list[LeadLagRow],
    row_count: int,
) -> LeadLagSummary:
    deltas = [row.delta_magnitude for row in rows if row.delta_magnitude is not None]
    return LeadLagSummary(
        options.transition,
        options.axis,
        options.tolerance_magnitude,
        row_count,
        sum(1 for row in rows if row.direction is LeadLagDirection.TARGET_LEADS),
        sum(1 for row in rows if row.direction is LeadLagDirection.TARGET_LAGS),
        sum(1 for row in rows if row.direction is LeadLagDirection.EQUAL),
        sum(1 for row in rows if row.direction is LeadLagDirection.MISSING_COMPARISON),
        sum(1 for row in rows if not row.is_within_tolerance),
        min(deltas) if deltas else None,
        max(deltas) if deltas else None,
    )


def _as_of_rows(
    window_name: str,
    key: Any,
    partition: Any,
    targets: Iterable[_NormalizedWindow],
    against: Iterable[_NormalizedWindow],
    options: _AsOfOptions,
    diagnostics: list[str],
) -> list[AsOfRow]:
    candidates = sorted(
        (
            _TransitionPoint(_primary_record_id(item), item.range.start, item.finality)
            for item in against
            if item.range.axis is options.axis
        ),
        key=lambda item: (item.point, item.record_id.value),
    )
    rows: list[AsOfRow] = []

    for target in targets:
        if target.range.axis is not options.axis:
            continue
        target_point = target.range.start
        if not candidates:
            rows.append(
                _create_as_of_row(target, options, window_name, key, partition, None, None)
            )
            continue

        candidate, ambiguous, future_rejected = _find_as_of_candidate(
            candidates,
            target_point,
            options,
        )
        if candidate is None:
            distance = (
                abs(_delta_magnitude(target_point, future_rejected.point, options.axis))
                if future_rejected is not None
                else None
            )
            rows.append(
                _create_as_of_row(
                    target,
                    options,
                    window_name,
                    key,
                    partition,
                    None,
                    distance,
                    AsOfMatchStatus.FUTURE_REJECTED
                    if future_rejected is not None
                    else AsOfMatchStatus.NO_MATCH,
                )
            )
            continue

        distance = abs(_delta_magnitude(target_point, candidate.point, options.axis))
        if distance > options.tolerance_magnitude:
            rows.append(
                _create_as_of_row(
                    target,
                    options,
                    window_name,
                    key,
                    partition,
                    None,
                    distance,
                    AsOfMatchStatus.NO_MATCH,
                )
            )
            continue

        if ambiguous:
            diagnostics.append("ambiguous_as_of_match")
        status = (
            AsOfMatchStatus.AMBIGUOUS
            if ambiguous
            else AsOfMatchStatus.EXACT
            if distance == 0
            else AsOfMatchStatus.MATCHED
        )
        rows.append(
            _create_as_of_row(
                target,
                options,
                window_name,
                key,
                partition,
                candidate,
                distance,
                status,
            )
        )
    return rows


def _create_as_of_row(
    target: _NormalizedWindow,
    options: _AsOfOptions,
    window_name: str,
    key: Any,
    partition: Any,
    match: _TransitionPoint | None,
    distance: int | None,
    status: AsOfMatchStatus = AsOfMatchStatus.NO_MATCH,
) -> AsOfRow:
    return AsOfRow(
        window_name,
        key,
        partition,
        options.axis,
        options.direction,
        target.range.start,
        match.point if match is not None else None,
        distance,
        options.tolerance_magnitude,
        status,
        _primary_record_id(target),
        match.record_id if match is not None else None,
        ComparisonFinality.PROVISIONAL
        if target.finality is ComparisonFinality.PROVISIONAL
        or (match is not None and match.finality is ComparisonFinality.PROVISIONAL)
        else ComparisonFinality.FINAL,
    )


def _find_as_of_candidate(
    candidates: list[_TransitionPoint],
    target_point: TemporalPoint,
    options: _AsOfOptions,
) -> tuple[_TransitionPoint | None, bool, _TransitionPoint | None]:
    ambiguous = False
    future_rejected: _TransitionPoint | None = None
    best: _TransitionPoint | None = None
    best_distance: int | None = None

    for candidate in candidates:
        comparison = candidate.point.compare_to(target_point)
        if options.direction is AsOfDirection.PREVIOUS and comparison > 0:
            future_rejected = future_rejected or candidate
            continue
        if options.direction is AsOfDirection.NEXT and comparison < 0:
            continue

        distance = abs(_delta_magnitude(target_point, candidate.point, options.axis))
        if best_distance is None or distance < best_distance:
            best = candidate
            best_distance = distance
            ambiguous = False
            continue
        if distance == best_distance:
            ambiguous = True
            if best is not None and candidate.record_id.value < best.record_id.value:
                best = candidate

    return best, ambiguous, future_rejected


def _transition_point(
    temporal_range: TemporalRange,
    transition: LeadLagTransition,
) -> TemporalPoint | None:
    if transition is LeadLagTransition.START:
        return temporal_range.start
    return temporal_range.end


def _nearest_transition(
    candidates: list[_TransitionPoint],
    target_point: TemporalPoint,
    axis: TemporalAxis,
) -> _TransitionPoint:
    best = candidates[0]
    best_distance = abs(_delta_magnitude(target_point, best.point, axis))
    for candidate in candidates[1:]:
        distance = abs(_delta_magnitude(target_point, candidate.point, axis))
        if distance < best_distance:
            best = candidate
            best_distance = distance
    return best


def _delta_magnitude(
    target_point: TemporalPoint,
    comparison_point: TemporalPoint,
    axis: TemporalAxis,
) -> int:
    if axis is TemporalAxis.TIMESTAMP:
        delta = target_point.timestamp - comparison_point.timestamp
        return int(delta.total_seconds() * 1_000_000)
    return target_point.position - comparison_point.position


def _lead_lag_direction(delta: int) -> LeadLagDirection:
    if delta < 0:
        return LeadLagDirection.TARGET_LEADS
    if delta > 0:
        return LeadLagDirection.TARGET_LAGS
    return LeadLagDirection.EQUAL


def _merged_ranges(ranges: list[TemporalRange]) -> list[float]:
    return [item.magnitude() for item in _merge_temporal_ranges(ranges)]


def _merge_temporal_ranges(ranges: list[TemporalRange]) -> list[TemporalRange]:
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda item: item.start)
    merged: list[TemporalRange] = []
    for item in ordered:
        if not merged:
            merged.append(item)
            continue
        previous = merged[-1]
        if item.start <= previous.require_end():
            end = max(previous.require_end(), item.require_end())
            merged[-1] = TemporalRange(previous.start, end, previous.end_status)
        else:
            merged.append(item)
    return merged


def _finality(*items: _NormalizedWindow) -> ComparisonFinality:
    return (
        ComparisonFinality.PROVISIONAL
        if any(item.finality is ComparisonFinality.PROVISIONAL for item in items)
        else ComparisonFinality.FINAL
    )


def _result_row_groups(result: ComparisonResult) -> tuple[tuple[str, tuple[Any, ...]], ...]:
    return (
        ("overlap", result.overlap_rows),
        ("residual", result.residual_rows),
        ("missing", result.missing_rows),
        ("coverage", result.coverage_rows),
        ("gap", result.gap_rows),
        ("symmetric_difference", result.symmetric_difference_rows),
        ("containment", result.containment_rows),
        ("lead_lag", result.lead_lag_rows),
        ("as_of", result.as_of_rows),
    )


def _finality_key(row: ComparisonRowFinality) -> str:
    return f"{row.row_type}\n{row.row_id}"


def _append_once(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _diagnostic_from_code(code: str, *, strict: bool) -> ComparisonDiagnostic:
    severity = _diagnostic_severity(code)
    if strict and severity is ComparisonDiagnosticSeverity.WARNING:
        severity = ComparisonDiagnosticSeverity.ERROR
    return ComparisonDiagnostic(
        code,
        _DIAGNOSTIC_MESSAGES.get(code, "Comparison diagnostic."),
        _DIAGNOSTIC_PATHS.get(code, "comparison"),
        severity,
    )


def _diagnostic_severity(code: str) -> ComparisonDiagnosticSeverity:
    if code in {
        "ambiguous_as_of_match",
        "duplicate_window",
        "future_leakage_risk",
        "future_window_excluded",
    }:
        return ComparisonDiagnosticSeverity.WARNING
    return ComparisonDiagnosticSeverity.ERROR


_DIAGNOSTIC_MESSAGES = {
    "ambiguous_as_of_match": "As-of lookup found multiple equally near comparison windows.",
    "duplicate_window": "Duplicate normalized windows were encountered.",
    "future_leakage_risk": (
        "Point-in-time lookup without known-at may use unavailable future records."
    ),
    "future_window_excluded": "A window was not available at the configured known-at point.",
    "invalid_range_duration": "A normalized temporal range had an invalid duration.",
    "known_at_requires_processing_position": (
        "Known-at filtering currently requires processing-position availability."
    ),
    "missing_event_time": "Event-time comparison requires recorded event timestamps.",
    "open_windows_without_policy": "Open windows require an explicit evaluation horizon.",
}


_DIAGNOSTIC_PATHS = {
    "ambiguous_as_of_match": "comparators.as_of",
    "duplicate_window": "normalization.duplicate_windows",
    "future_leakage_risk": "normalization.known_at",
    "future_window_excluded": "normalization.known_at",
    "invalid_range_duration": "normalization.horizon",
    "known_at_requires_processing_position": "normalization.known_at",
    "missing_event_time": "normalization.axis",
    "open_windows_without_policy": "normalization.horizon",
}


def _missing_event_time(window: WindowRecord) -> bool:
    return window.start_time is None or (window.is_closed and window.end_time is None)


def _count_for(
    comparator: str,
    overlap_rows: list[OverlapRow],
    residual_rows: list[ResidualRow],
    missing_rows: list[MissingRow],
    coverage_rows: list[CoverageRow],
    gap_rows: list[GapRow],
    symmetric_difference_rows: list[SymmetricDifferenceRow],
    containment_rows: list[ContainmentRow],
    lead_lag_rows: list[LeadLagRow],
    lead_lag_counts: dict[str, int],
    as_of_rows: list[AsOfRow],
    as_of_counts: dict[str, int],
) -> int:
    if comparator in lead_lag_counts:
        return lead_lag_counts[comparator]
    if comparator in as_of_counts:
        return as_of_counts[comparator]
    return {
        "overlap": len(overlap_rows),
        "residual": len(residual_rows),
        "missing": len(missing_rows),
        "coverage": len(coverage_rows),
        "gap": len(gap_rows),
        "symmetric_difference": len(symmetric_difference_rows),
        "containment": len(containment_rows),
        "lead_lag": len(lead_lag_rows),
        "as_of": len(as_of_rows),
    }.get(comparator, 0)


def _normalize_comparator(comparator: str) -> str:
    return comparator.strip().lower().replace("-", "_")


def _try_parse_lead_lag(comparator: str) -> _LeadLagOptions | None:
    parts = comparator.split(":")
    if len(parts) != 4 or parts[0] != "lead_lag":
        return None

    transition = {
        "start": LeadLagTransition.START,
        "end": LeadLagTransition.END,
    }.get(parts[1].replace("_", ""))
    axis = _parse_axis(parts[2])
    try:
        tolerance = int(parts[3])
    except ValueError:
        return None
    if transition is None or axis is None or tolerance < 0:
        return None
    return _LeadLagOptions(transition, axis, tolerance)


def _try_parse_as_of(comparator: str) -> _AsOfOptions | None:
    parts = comparator.split(":")
    if len(parts) != 4 or parts[0] not in {"asof", "as_of"}:
        return None

    direction = {
        "previous": AsOfDirection.PREVIOUS,
        "next": AsOfDirection.NEXT,
        "nearest": AsOfDirection.NEAREST,
    }.get(parts[1].replace("_", ""))
    axis = _parse_axis(parts[2])
    try:
        tolerance = int(parts[3])
    except ValueError:
        return None
    if direction is None or axis is None or tolerance < 0:
        return None
    return _AsOfOptions(direction, axis, tolerance)


def _parse_axis(value: str) -> TemporalAxis | None:
    normalized = value.replace("_", "")
    if normalized == "processingposition":
        return TemporalAxis.PROCESSING_POSITION
    if normalized == "timestamp":
        return TemporalAxis.TIMESTAMP
    return None


def _row_sort_key(row: Any) -> tuple[str, str, str, TemporalPoint, TemporalPoint]:
    return (
        repr(row.window_name),
        repr(row.key),
        repr(row.partition),
        row.range.start,
        row.range.require_end(),
    )


def _range_label(temporal_range: TemporalRange) -> str:
    end = temporal_range.require_end()
    if temporal_range.axis is TemporalAxis.PROCESSING_POSITION:
        return f"[{temporal_range.start.position}, {end.position})"
    return f"[{temporal_range.start.timestamp.isoformat()}, {end.timestamp.isoformat()})"


def _row_range_or_point(row: Any) -> str:
    if isinstance(row, LeadLagRow | AsOfRow):
        return _point_label(row.target_point)
    return _range_label(row.range)


def _row_record_ids(row: Any) -> tuple[WindowRecordId, ...]:
    ids: list[WindowRecordId] = []
    for name in (
        "target_record_ids",
        "against_record_ids",
        "container_record_ids",
        "parent_record_ids",
        "child_record_ids",
    ):
        ids.extend(getattr(row, name, ()))
    for name in ("target_record_id", "comparison_record_id", "matched_record_id"):
        record_id = getattr(row, name, None)
        if record_id is not None:
            ids.append(record_id)
    return tuple(ids)


def _parse_cohort_evidence(
    metadata: ComparisonExtensionMetadata,
) -> CohortEvidenceMetadata | None:
    prefix = "segment["
    if not metadata.key.startswith(prefix) or not metadata.key.endswith("]"):
        return None
    try:
        segment_index = int(metadata.key[len(prefix) : -1])
    except ValueError:
        return None
    fields = _parse_metadata_fields(metadata.value)
    try:
        rule = fields["rule"]
        required_count = int(fields["required"])
        active_count = int(fields["activeCount"])
        is_active = fields["isActive"].lower() == "true"
    except (KeyError, ValueError):
        return None
    active_sources = tuple(
        source
        for source in fields.get("activeSources", "").split(",")
        if source
    )
    return CohortEvidenceMetadata(
        segment_index,
        rule,
        required_count,
        active_count,
        is_active,
        active_sources,
        metadata.value,
    )


def _parse_metadata_fields(value: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in value.split(";"):
        key, separator, field_value = part.strip().partition("=")
        if not separator or not key:
            continue
        fields[key] = field_value
    return fields


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, ComparisonResult):
        payload = {
            key: _to_jsonable(item)
            for key, item in asdict(value).items()
        }
        payload["diagnostic_rows"] = _to_jsonable(value.diagnostic_rows)
        payload["row_finalities"] = _to_jsonable(value.row_finalities)
        payload["is_valid"] = value.is_valid
        return payload
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, WindowRecordId):
        return value.value
    if isinstance(value, TemporalPoint):
        if value.axis is TemporalAxis.PROCESSING_POSITION:
            return {"axis": value.axis.value, "position": value.position}
        return {
            "axis": value.axis.value,
            "timestamp": value.timestamp.isoformat(),
            "clock": value.clock,
        }
    if isinstance(value, TemporalRange):
        return {
            "start": _to_jsonable(value.start),
            "end": _to_jsonable(value.end) if value.end is not None else None,
            "end_status": value.end_status.value,
        }
    if hasattr(value, "__dataclass_fields__"):
        return {key: _to_jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, tuple | list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    return value


def _html_row(label: str, color: str, row: Any) -> str:
    if isinstance(row, LeadLagRow | AsOfRow):
        start = (
            row.target_point.position
            if row.target_point.axis is TemporalAxis.PROCESSING_POSITION
            else 0
        )
        end = start + 1
        range_text = _point_label(row.target_point)
    else:
        start = (
            row.range.start.position
            if row.range.axis is TemporalAxis.PROCESSING_POSITION
            else 0
        )
        end = (
            row.range.require_end().position
            if row.range.axis is TemporalAxis.PROCESSING_POSITION
            else 1
        )
        range_text = _range_label(row.range)
    width = max(end - start, 1)
    left = min(start * 4, 92)
    bar_width = min(width * 4, 100 - left)
    return (
        '<div class="row">'
        f'<div class="meta"><strong>{html.escape(label)}</strong><br>'
        f"{html.escape(str(row.window_name))} / {html.escape(str(row.key))}<br>"
        f"{html.escape(range_text)}<br>"
        f"{html.escape(getattr(row, 'finality', ComparisonFinality.FINAL).value)}</div>"
        '<div class="track">'
        f'<div class="bar" style="left:{left}%;width:{bar_width}%;background:{color}"></div>'
        "</div></div>"
    )


def _point_label(point: TemporalPoint) -> str:
    if point.axis is TemporalAxis.PROCESSING_POSITION:
        return str(point.position)
    return point.timestamp.isoformat()
