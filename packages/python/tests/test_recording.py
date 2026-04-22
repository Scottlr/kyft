from dataclasses import dataclass

from spanfold import Spanfold, WindowBoundaryReason, WindowTransitionKind


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
            tags=lambda event: {"fleet": "warehouse"},
        )
    )
    pipeline.ingest(DeviceStatus("device-17", False), source="provider-a", partition="p1")
    pipeline.ingest(DeviceStatus("device-17", True), source="provider-a", partition="p1")

    rows = (
        pipeline.history.query()
        .where_window("DeviceOffline")
        .where_key("device-17")
        .where_source("provider-a")
        .where_partition("p1")
        .where_tag("fleet", "warehouse")
        .closed()
        .all()
    )

    assert len(rows) == 1
    assert pipeline.history.query().latest() == rows[0]
