from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from spanfold import (
    AsOfDirection,
    AsOfMatchStatus,
    ComparisonChangelog,
    ComparisonDiagnosticSeverity,
    ComparisonFinality,
    ComparisonRowFinality,
    ComparisonSide,
    ContainmentStatus,
    LeadLagDirection,
    LeadLagTransition,
    Spanfold,
    TemporalAxis,
    TemporalPoint,
)
from spanfold.testing import WindowHistoryFixtureBuilder


@dataclass(frozen=True)
class DeviceStatus:
    device_id: str
    is_online: bool


@dataclass(frozen=True)
class TimedDeviceStatus:
    device_id: str
    is_online: bool
    occurred_at: datetime


def _pipeline():
    return (
        Spanfold.for_events()
        .record_windows()
        .track_window(
            "DeviceOffline",
            key=lambda event: event.device_id,
            is_active=lambda event: not event.is_online,
        )
    )


def test_overlap_residual_missing_and_coverage() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")  # [1, 5)
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")  # [3, 7)
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-a")
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-b")

    result = (
        pipeline.history.compare("Provider QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("overlap", "residual", "missing", "coverage")
        .run()
    )

    assert [
        (row.range.start.position, row.range.require_end().position) for row in result.overlap_rows
    ] == [(3, 5)]
    assert [
        (row.range.start.position, row.range.require_end().position) for row in result.residual_rows
    ] == [(1, 3)]
    assert [
        (row.range.start.position, row.range.require_end().position) for row in result.missing_rows
    ] == [(5, 7)]
    assert result.coverage_rows[0].target_magnitude == 4
    assert result.coverage_rows[0].covered_magnitude == 2
    assert result.coverage_rows[0].coverage_ratio == 0.5


def test_open_windows_are_excluded_until_horizon_is_explicit() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")

    excluded = (
        pipeline.history.compare("Live")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("overlap")
        .run()
    )
    included = (
        pipeline.history.compare("Live")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(5))
        .using("overlap")
        .run()
    )

    assert not excluded.overlap_rows
    assert included.overlap_rows[0].finality is ComparisonFinality.PROVISIONAL
    assert included.overlap_rows[0].range.start.position == 2
    assert included.overlap_rows[0].range.require_end().position == 5


def test_run_live_clips_open_windows_to_horizon() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")

    result = (
        pipeline.history.compare("Live")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("overlap")
        .run_live(TemporalPoint.for_position(5))
    )

    assert result.evaluation_horizon == TemporalPoint.for_position(5)
    assert result.overlap_rows[0].finality is ComparisonFinality.PROVISIONAL
    assert result.has_provisional_rows()


def test_result_exposes_row_finality_snapshot() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")

    result = (
        pipeline.history.compare("Live")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(5))
        .using("overlap")
        .run()
    )

    finalities = result.row_finalities
    assert result.has_provisional_rows()
    assert [(row.row_type, row.row_id, row.finality) for row in finalities] == [
        ("overlap", "overlap[0]", ComparisonFinality.PROVISIONAL)
    ]
    assert "open window" in finalities[0].reason


def test_comparison_changelog_revises_and_retracts_rows() -> None:
    previous = (
        ComparisonRowFinality(
            "missing",
            "missing[0]",
            ComparisonFinality.FINAL,
            "All contributing windows were closed when the row was produced.",
        ),
        ComparisonRowFinality(
            "overlap",
            "overlap[0]",
            ComparisonFinality.PROVISIONAL,
            "Depends on at least one open window clipped to the evaluation horizon.",
        ),
    )
    current = (
        ComparisonRowFinality(
            "overlap",
            "overlap[0]",
            ComparisonFinality.FINAL,
            "All contributing windows were closed when the row was produced.",
        ),
    )

    entries = ComparisonChangelog.create(previous, current)

    assert [(entry.row_type, entry.finality, entry.version) for entry in entries] == [
        ("overlap", ComparisonFinality.REVISED, 2),
        ("missing", ComparisonFinality.RETRACTED, 2),
    ]
    assert entries[0].supersedes_row_id == "overlap[0]"
    assert ComparisonChangelog.replay(previous, entries) == (
        ComparisonRowFinality(
            "overlap",
            "overlap[0]",
            ComparisonFinality.FINAL,
            entries[0].reason,
            2,
            "overlap[0]",
        ),
    )


def test_known_at_filters_windows_before_comparison_and_exports_metadata() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-b")

    early = (
        pipeline.history.compare("Known-at QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(known_at=TemporalPoint.for_position(2))
        .using("overlap")
        .run()
    )
    known = (
        pipeline.history.compare("Known-at QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(known_at=TemporalPoint.for_position(4))
        .using("overlap")
        .run()
    )

    assert not early.overlap_rows
    assert "future_window_excluded" in early.diagnostics
    assert len(known.overlap_rows) == 1
    assert "future_window_excluded" not in known.diagnostics
    assert '"known_at": {' in known.to_json()
    assert "knownAt=4" in known.to_markdown()


def test_event_time_mode_diagnoses_missing_timestamps() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-b")

    result = (
        pipeline.history.compare("Event-time QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(axis=TemporalAxis.TIMESTAMP)
        .using("overlap")
        .run()
    )

    assert not result.overlap_rows
    assert "missing_event_time" in result.diagnostics


def test_event_time_mode_compares_recorded_timestamps() -> None:
    start = datetime(2026, 4, 21, 10, tzinfo=UTC)
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .with_event_time(lambda event: event.occurred_at)
        .track_window(
            "DeviceOffline",
            key=lambda event: event.device_id,
            is_active=lambda event: not event.is_online,
        )
    )
    pipeline.ingest(TimedDeviceStatus("device-1", False, start), source="provider-a")
    pipeline.ingest(
        TimedDeviceStatus("device-1", False, start + timedelta(minutes=2)),
        source="provider-b",
    )
    pipeline.ingest(
        TimedDeviceStatus("device-1", True, start + timedelta(minutes=5)),
        source="provider-a",
    )
    pipeline.ingest(
        TimedDeviceStatus("device-1", True, start + timedelta(minutes=7)),
        source="provider-b",
    )

    result = (
        pipeline.history.compare("Event-time QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(axis=TemporalAxis.TIMESTAMP)
        .using("overlap")
        .run()
    )

    assert not result.diagnostics
    assert result.overlap_rows[0].range.start.timestamp == start + timedelta(minutes=2)
    assert result.overlap_rows[0].range.require_end().timestamp == start + timedelta(minutes=5)


def test_normalization_can_reject_duplicate_windows() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("DeviceOffline", "device-1", 1, 5, source="provider-a")
        .add_closed_window("DeviceOffline", "device-1", 1, 5, source="provider-a")
        .add_closed_window("DeviceOffline", "device-1", 1, 5, source="provider-b")
        .build()
    )

    result = (
        history.compare("Duplicate QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(duplicate_windows="reject")
        .using("overlap")
        .run()
    )

    assert len(result.overlap_rows) == 1
    assert "duplicate_window" in result.diagnostics


def test_normalization_can_coalesce_adjacent_windows() -> None:
    history = (
        WindowHistoryFixtureBuilder()
        .add_closed_window("DeviceOffline", "device-1", 1, 3, source="provider-a")
        .add_closed_window("DeviceOffline", "device-1", 3, 5, source="provider-a")
        .add_closed_window("DeviceOffline", "device-1", 1, 5, source="provider-b")
        .build()
    )

    result = (
        history.compare("Coalesce QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(coalesce_adjacent=True)
        .using("overlap")
        .run()
    )

    assert len(result.overlap_rows) == 1
    assert result.overlap_rows[0].range.start.position == 1
    assert result.overlap_rows[0].range.require_end().position == 5
    assert len(result.overlap_rows[0].target_record_ids) == 2


def test_exports_are_deterministic() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")
    result = (
        pipeline.history.compare("Provider QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(3))
        .using("overlap")
        .run()
    )

    assert '"name": "Provider QA"' in result.to_json()
    assert '"row_finalities": [' in result.to_json()
    assert '"row_type": "overlap"' in result.to_json_lines()
    assert "| overlap | 1 |" in result.to_markdown()
    assert "<html" in result.to_debug_html()


def test_gap_detects_internal_uncovered_space() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")  # [1, 3)
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-a")
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")  # [5, 7)
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-b")

    result = (
        pipeline.history.compare("Provider QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("gap")
        .run()
    )

    assert _ranges(result.gap_rows) == [(3, 5)]
    assert result.comparator_summaries[0].row_count == 1


def test_gap_does_not_invent_boundary_gaps() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-a")

    result = (
        pipeline.history.compare("Provider QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("gap")
        .run()
    )

    assert not result.gap_rows


def test_symmetric_difference_includes_both_disagreement_sides() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")  # [1, 5)
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")  # [3, 7)
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-a")
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", True), source="provider-b")

    result = (
        pipeline.history.compare("Provider QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("symmetric-difference")
        .run()
    )

    assert [row.side for row in result.symmetric_difference_rows] == [
        ComparisonSide.TARGET,
        ComparisonSide.AGAINST,
    ]
    assert _ranges(result.symmetric_difference_rows) == [(1, 3), (5, 7)]


def test_containment_splits_overhangs_and_contained_ranges() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="target")  # [1, 7)
    pipeline.ingest(DeviceStatus("noise", True), source="target")
    pipeline.ingest(DeviceStatus("device-1", False), source="container")  # [3, 5)
    pipeline.ingest(DeviceStatus("noise", True), source="target")
    pipeline.ingest(DeviceStatus("device-1", True), source="container")
    pipeline.ingest(DeviceStatus("noise", True), source="target")
    pipeline.ingest(DeviceStatus("device-1", True), source="target")

    result = (
        pipeline.history.compare("Containment QA")
        .target("target")
        .against("container")
        .within(window_name="DeviceOffline")
        .using("containment")
        .run()
    )

    assert [row.status for row in result.containment_rows] == [
        ContainmentStatus.LEFT_OVERHANG,
        ContainmentStatus.CONTAINED,
        ContainmentStatus.RIGHT_OVERHANG,
    ]
    assert _ranges(result.containment_rows) == [(1, 3), (3, 5), (5, 7)]


def test_multiple_containers_contribute_contained_rows() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="target")  # [1, 7)
    pipeline.ingest(DeviceStatus("device-1", False), source="container")  # [2, 4)
    pipeline.ingest(DeviceStatus("device-1", True), source="container")
    pipeline.ingest(DeviceStatus("device-1", False), source="container")  # [4, 6)
    pipeline.ingest(DeviceStatus("device-1", True), source="container")
    pipeline.ingest(DeviceStatus("device-1", True), source="target")

    result = (
        pipeline.history.compare("Containment QA")
        .target("target")
        .against("container")
        .within(window_name="DeviceOffline")
        .using("containment")
        .run()
    )

    assert sum(
        1 for row in result.containment_rows if row.status is ContainmentStatus.CONTAINED
    ) == 2


def test_lead_lag_reports_target_leads_against() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="target")  # start 1
    pipeline.ingest(DeviceStatus("noise", True), source="target")
    pipeline.ingest(DeviceStatus("device-1", False), source="comparison")  # start 3

    result = (
        pipeline.history.compare("Latency QA")
        .target("target")
        .against("comparison")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(6))
        .using("lead-lag:start:processing-position:5")
        .run()
    )

    row = result.lead_lag_rows[0]
    assert row.transition is LeadLagTransition.START
    assert row.direction is LeadLagDirection.TARGET_LEADS
    assert row.delta_magnitude == -2
    assert row.is_within_tolerance
    assert result.lead_lag_summaries[0].target_lead_count == 1


def test_lead_lag_reports_target_lags_and_outside_tolerance() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="comparison")  # start 1
    pipeline.ingest(DeviceStatus("noise", True), source="comparison")
    pipeline.ingest(DeviceStatus("noise", True), source="comparison")
    pipeline.ingest(DeviceStatus("noise", True), source="comparison")
    pipeline.ingest(DeviceStatus("device-1", False), source="target")  # start 5

    result = (
        pipeline.history.compare("Latency QA")
        .target("target")
        .against("comparison")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(7))
        .using("lead_lag:start:processing_position:1")
        .run()
    )

    row = result.lead_lag_rows[0]
    assert row.direction is LeadLagDirection.TARGET_LAGS
    assert row.delta_magnitude == 4
    assert not row.is_within_tolerance
    assert result.lead_lag_summaries[0].outside_tolerance_count == 1


def test_lead_lag_can_compare_end_transitions() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="comparison")
    pipeline.ingest(DeviceStatus("device-1", False), source="target")
    pipeline.ingest(DeviceStatus("noise", True), source="target")
    pipeline.ingest(DeviceStatus("device-1", True), source="comparison")  # end 4
    pipeline.ingest(DeviceStatus("noise", True), source="target")
    pipeline.ingest(DeviceStatus("device-1", True), source="target")  # end 6

    result = (
        pipeline.history.compare("Latency QA")
        .target("target")
        .against("comparison")
        .within(window_name="DeviceOffline")
        .using("lead-lag:end:processing-position:5")
        .run()
    )

    row = result.lead_lag_rows[0]
    assert row.transition is LeadLagTransition.END
    assert row.direction is LeadLagDirection.TARGET_LAGS
    assert row.delta_magnitude == 2


def test_lead_lag_reports_missing_comparison_transition() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="target")

    result = (
        pipeline.history.compare("Latency QA")
        .target("target")
        .against("comparison")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(5))
        .using("lead-lag:start:processing-position:5")
        .run()
    )

    row = result.lead_lag_rows[0]
    assert row.direction is LeadLagDirection.MISSING_COMPARISON
    assert row.delta_magnitude is None
    assert row.comparison_point is None
    assert not row.is_within_tolerance


def test_as_of_emits_previous_match_within_tolerance() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="quote")  # start 1
    pipeline.ingest(DeviceStatus("noise", True), source="quote")
    pipeline.ingest(DeviceStatus("device-1", False), source="trade")  # start 3

    result = (
        pipeline.history.compare("Quote at trade")
        .target("trade")
        .against("quote")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(6))
        .using("asof:previous:processing-position:5")
        .run()
    )

    row = result.as_of_rows[0]
    assert row.direction is AsOfDirection.PREVIOUS
    assert row.status is AsOfMatchStatus.MATCHED
    assert row.distance_magnitude == 2
    assert row.matched_point is not None
    assert row.matched_point.position == 1


def test_as_of_rejects_future_match_for_previous_direction() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="trade")  # start 1
    pipeline.ingest(DeviceStatus("device-1", False), source="quote")  # start 2

    result = (
        pipeline.history.compare("Quote at trade")
        .target("trade")
        .against("quote")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(4))
        .using("asof:previous:processing-position:5")
        .run()
    )

    row = result.as_of_rows[0]
    assert row.status is AsOfMatchStatus.FUTURE_REJECTED
    assert row.distance_magnitude == 1
    assert row.matched_record_id is None


def test_as_of_can_match_next_transition() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="trade")  # start 1
    pipeline.ingest(DeviceStatus("noise", True), source="trade")
    pipeline.ingest(DeviceStatus("device-1", False), source="quote")  # start 3

    result = (
        pipeline.history.compare("Quote at trade")
        .target("trade")
        .against("quote")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(5))
        .using("asof:next:processing-position:5")
        .run()
    )

    assert result.as_of_rows[0].status is AsOfMatchStatus.MATCHED
    assert result.as_of_rows[0].distance_magnitude == 2


def test_as_of_reports_no_match_outside_tolerance() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="quote")  # start 1
    pipeline.ingest(DeviceStatus("noise", True), source="quote")
    pipeline.ingest(DeviceStatus("noise", True), source="quote")
    pipeline.ingest(DeviceStatus("noise", True), source="quote")
    pipeline.ingest(DeviceStatus("device-1", False), source="trade")  # start 5

    result = (
        pipeline.history.compare("Quote at trade")
        .target("trade")
        .against("quote")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(7))
        .using("asof:previous:processing-position:2")
        .run()
    )

    assert result.as_of_rows[0].status is AsOfMatchStatus.NO_MATCH
    assert result.as_of_rows[0].distance_magnitude == 4


def test_as_of_nearest_ambiguous_match_is_diagnosed() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="quote-a")  # start 1
    pipeline.ingest(DeviceStatus("noise", True), source="quote-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="trade")  # start 3
    pipeline.ingest(DeviceStatus("noise", True), source="quote-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="quote-b")  # start 5

    result = (
        pipeline.history.compare("Quote at trade")
        .target("trade")
        .against(predicate=lambda window: window.source in {"quote-a", "quote-b"})
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(7))
        .using("asof:nearest:processing-position:5")
        .run()
    )

    assert result.as_of_rows[0].status is AsOfMatchStatus.AMBIGUOUS
    assert result.as_of_rows[0].distance_magnitude == 2
    assert "ambiguous_as_of_match" in result.diagnostics
    assert "future_leakage_risk" in result.diagnostics


def test_diagnostics_expose_structured_severity_and_strict_promotion() -> None:
    pipeline = _pipeline()
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-a")
    pipeline.ingest(DeviceStatus("noise", True), source="provider-a")
    pipeline.ingest(DeviceStatus("device-1", False), source="provider-b")

    result = (
        pipeline.history.compare("As-of QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(5))
        .using("asof:next:processing-position:10")
        .run()
    )
    strict = (
        pipeline.history.compare("As-of QA")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .normalize(horizon=TemporalPoint.for_position(5))
        .using("asof:next:processing-position:10")
        .strict()
        .run()
    )

    assert result.is_valid
    assert result.diagnostic_rows[0].code == "future_leakage_risk"
    assert result.diagnostic_rows[0].severity is ComparisonDiagnosticSeverity.WARNING
    assert '"diagnostic_rows": [' in result.to_json()
    assert not strict.is_valid
    assert strict.diagnostic_rows[0].severity is ComparisonDiagnosticSeverity.ERROR


def _ranges(rows) -> list[tuple[int, int]]:  # type: ignore[no-untyped-def]
    return [(row.range.start.position, row.range.require_end().position) for row in rows]
