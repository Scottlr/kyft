from dataclasses import dataclass

from spanfold import Spanfold


@dataclass(frozen=True)
class DeviceStatus:
    device_id: str
    is_online: bool
    region: str


pipeline = (
    Spanfold.for_events()
    .record_windows()
    .track_window(
        "DeviceOffline",
        key=lambda event: event.device_id,
        is_active=lambda event: not event.is_online,
        segments=lambda event: {"region": event.region},
        tags=lambda event: {"domain": "monitoring"},
    )
)

pipeline.ingest(DeviceStatus("device-17", False, "north"), source="provider-a")
pipeline.ingest(DeviceStatus("device-17", True, "north"), source="provider-a")

for window in pipeline.history.closed_windows:
    print(window.window_name, window.key, window.source, window.start_position, window.end_position)
