from dataclasses import dataclass

import pytest

from spanfold import (
    Spanfold,
    TemporalPoint,
    WindowBoundaryReason,
    WindowGroupKind,
    WindowTransitionKind,
)


@dataclass(frozen=True)
class DeviceStatus:
    device_id: str
    is_online: bool
    region: str = "north"


def test_pipeline_records_open_and_closed_windows_by_source() -> None:
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .track_window(
            "DeviceOffline",
            key=lambda event: event.device_id,
            is_active=lambda event: not event.is_online,
        )
    )

    first = pipeline.ingest(DeviceStatus("device-17", False), source="provider-a")
    second = pipeline.ingest(DeviceStatus("device-17", True), source="provider-a")

    assert first.emissions[0].kind is WindowTransitionKind.OPENED
    assert second.emissions[0].kind is WindowTransitionKind.CLOSED
    window = pipeline.history.closed_windows[0]
    assert window.window_name == "DeviceOffline"
    assert window.key == "device-17"
    assert window.source == "provider-a"
    assert window.start_position == 1
    assert window.end_position == 2


def test_source_owns_independent_runtime_state() -> None:
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .track_window(
            "DeviceOffline",
            key=lambda event: event.device_id,
            is_active=lambda event: not event.is_online,
        )
    )

    pipeline.ingest(DeviceStatus("device-17", False), source="provider-a")
    pipeline.ingest(DeviceStatus("device-17", False), source="provider-b")
    pipeline.ingest(DeviceStatus("device-17", True), source="provider-b")

    assert len(pipeline.history.closed_windows) == 1
    assert len(pipeline.history.open_windows) == 1
    assert pipeline.history.open_windows[0].source == "provider-a"


def test_segment_change_closes_and_reopens_window() -> None:
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .track_window(
            "DeviceOffline",
            key=lambda event: event.device_id,
            is_active=lambda event: not event.is_online,
            segments=lambda event: {"region": event.region},
        )
    )

    result1 = pipeline.ingest(DeviceStatus("device-17", False, "north"))
    result2 = pipeline.ingest(DeviceStatus("device-17", False, "south"))

    assert len(result1.emissions) == 1
    assert [emission.kind for emission in result2.emissions] == [
        WindowTransitionKind.CLOSED,
        WindowTransitionKind.OPENED,
    ]
    closed = pipeline.history.closed_windows[0]
    assert closed.boundary_reason is WindowBoundaryReason.SEGMENT_CHANGED
    assert closed.boundary_changes[0].previous_value == "north"
    assert closed.boundary_changes[0].current_value == "south"


def test_direct_history_query_filters() -> None:
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .track_window(
            "DeviceOffline",
            key=lambda event: event.device_id,
            is_active=lambda event: not event.is_online,
            segments=lambda event: {"region": event.region},
            tags=lambda event: {"fleet": "warehouse"},
        )
    )
    pipeline.ingest(DeviceStatus("device-17", False), source="provider-a", partition="p1")
    pipeline.ingest(DeviceStatus("device-17", True), source="provider-a", partition="p1")
    pipeline.ingest(DeviceStatus("device-17", False, "south"), source="provider-b", partition="p1")
    pipeline.ingest(DeviceStatus("device-17", True, "south"), source="provider-b", partition="p1")

    rows = (
        pipeline.history.query()
        .where_window("DeviceOffline")
        .where_key("device-17")
        .where_lane("provider-a")
        .where_partition("p1")
        .where_segment("region", "north")
        .where_tag("fleet", "warehouse")
        .closed()
        .all()
    )

    assert len(rows) == 1
    assert rows[0].source == "provider-a"
    assert rows[0].segments[0].value == "north"
    assert pipeline.history.query().where_lane("provider-a").latest() == rows[0]


def test_recorded_windows_can_be_summarized_by_segment() -> None:
    pipeline = _segmented_pipeline()

    pipeline.ingest(DeviceStatus("device-1", False, "Incident"), source="lane-a")
    pipeline.ingest(DeviceStatus("device-1", True, "Incident"), source="lane-a")
    pipeline.ingest(DeviceStatus("device-2", False, "Incident"), source="lane-a")
    pipeline.ingest(DeviceStatus("device-3", False, "Normal"), source="lane-b")
    pipeline.ingest(DeviceStatus("device-3", True, "Normal"), source="lane-b")

    summaries = (
        pipeline.history.query()
        .where_window("DeviceOffline")
        .summarize_by_segment("lifecycle")
    )

    incident = next(summary for summary in summaries if summary.value == "Incident")
    assert incident.group_kind is WindowGroupKind.SEGMENT
    assert incident.name == "lifecycle"
    assert incident.record_count == 2
    assert incident.final_count == 1
    assert incident.provisional_count == 1
    assert incident.measured_position_count == 1
    assert incident.total_position_length == 1

    normal = next(summary for summary in summaries if summary.value == "Normal")
    assert normal.record_count == 1
    assert normal.final_count == 1
    assert normal.provisional_count == 0
    assert normal.total_position_length == 1


def test_recorded_windows_can_be_summarized_by_tag() -> None:
    pipeline = _segmented_pipeline()

    pipeline.ingest(DeviceStatus("device-1", False, "Incident"), source="lane-a")
    pipeline.ingest(DeviceStatus("device-1", True, "Incident"), source="lane-a")
    pipeline.ingest(DeviceStatus("device-2", False, "Normal"), source="lane-b")

    summaries = (
        pipeline.history.query()
        .where_window("DeviceOffline")
        .summarize_by_tag("fleet")
    )

    warehouse = next(summary for summary in summaries if summary.value == "warehouse")
    assert warehouse.group_kind is WindowGroupKind.TAG
    assert warehouse.name == "fleet"
    assert warehouse.record_count == 2
    assert warehouse.final_count == 1
    assert warehouse.provisional_count == 1
    assert warehouse.total_position_length == 1


def test_snapshot_windows_can_be_summarized_by_segment_with_horizon_length() -> None:
    pipeline = _segmented_pipeline()

    pipeline.ingest(DeviceStatus("device-1", False, "Incident"), source="lane-a")
    pipeline.ingest(DeviceStatus("device-1", True, "Incident"), source="lane-a")
    pipeline.ingest(DeviceStatus("device-2", False, "Incident"), source="lane-b")

    summaries = (
        pipeline.history.snapshot_at(TemporalPoint.for_position(6))
        .query()
        .where_window("DeviceOffline")
        .summarize_by_segment("lifecycle")
    )

    incident = summaries[0]
    assert incident.record_count == 2
    assert incident.final_count == 1
    assert incident.provisional_count == 1
    assert incident.measured_position_count == 2
    assert incident.total_position_length == 4


def test_summary_rejects_missing_dimension_name() -> None:
    with pytest.raises(ValueError, match="dimension name"):
        Spanfold.for_events().record_windows().build().history.query().summarize_by_segment("")


def _segmented_pipeline():
    return (
        Spanfold.for_events()
        .record_windows()
        .track_window(
            "DeviceOffline",
            key=lambda event: event.device_id,
            is_active=lambda event: not event.is_online,
            segments=lambda event: {"lifecycle": event.region},
            tags=lambda event: {"fleet": "warehouse"},
        )
    )
