from dataclasses import dataclass

from spanfold import Spanfold


@dataclass(frozen=True)
class DeviceSignal:
    device_id: str
    is_online: bool


def test_three_source_matrix_produces_expected_pairs() -> None:
    history = _build_history()

    matrix = history.compare_sources(
        "Provider matrix",
        "DeviceOffline",
        ["provider-a", "provider-b", "provider-c"],
    )

    assert len(matrix.sources) == 3
    assert len(matrix.cells) == 9
    assert matrix.try_get_cell("provider-a", "provider-b") is not None
    diagonal = matrix.get_cell("provider-c", "provider-c")
    assert diagonal.is_diagonal
    assert diagonal.coverage_ratio is None


def test_missing_source_cells_are_explicit() -> None:
    matrix = _build_history().compare_sources(
        "Provider matrix",
        "DeviceOffline",
        ["provider-a", "provider-c"],
    )

    cell = matrix.get_cell("provider-a", "provider-c")

    assert cell.target_has_windows
    assert not cell.against_has_windows
    assert cell.overlap_row_count == 0
    assert cell.residual_row_count == 1
    assert cell.missing_row_count == 0


def test_matrix_values_match_underlying_comparator_rows() -> None:
    history = _build_history()
    matrix = history.compare_sources(
        "Provider matrix",
        "DeviceOffline",
        ["provider-a", "provider-b"],
    )
    pair = matrix.get_cell("provider-a", "provider-b")

    result = (
        history.compare("Provider matrix provider-a vs provider-b")
        .target("provider-a")
        .against("provider-b")
        .within(window_name="DeviceOffline")
        .using("overlap", "residual", "missing", "coverage")
        .run()
    )

    assert pair.overlap_row_count == len(result.overlap_rows)
    assert pair.residual_row_count == len(result.residual_rows)
    assert pair.missing_row_count == len(result.missing_rows)
    assert pair.coverage_row_count == len(result.coverage_rows)


def test_matrix_cells_can_be_looked_up_directionally() -> None:
    matrix = _build_history().compare_sources(
        "Provider matrix",
        "DeviceOffline",
        ["provider-a", "provider-b"],
    )

    cell = matrix.get_cell("provider-a", "provider-b")

    assert not cell.is_diagonal
    assert matrix.try_get_cell("provider-b", "provider-a") is not None
    assert matrix.try_get_cell("provider-a", "provider-c") is None


def _build_history():
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .track_window(
            "DeviceOffline",
            key=lambda signal: signal.device_id,
            is_active=lambda signal: not signal.is_online,
        )
    )

    pipeline.ingest(DeviceSignal("device-1", False), source="provider-a")
    pipeline.ingest(DeviceSignal("device-1", True), source="provider-a")
    pipeline.ingest(DeviceSignal("device-1", False), source="provider-b")
    pipeline.ingest(DeviceSignal("device-1", True), source="provider-b")

    return pipeline.history
