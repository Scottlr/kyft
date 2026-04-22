"""Dependency-free benchmark harness for the Python Spanfold package.

Run from the repository root with:

    python packages/python/benchmarks/spanfold_benchmarks.py --smoke
    python packages/python/benchmarks/spanfold_benchmarks.py
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from spanfold import (  # noqa: E402
    CohortActivity,
    LeadLagTransition,
    Spanfold,
    TemporalAxis,
    TemporalPoint,
)


@dataclass(frozen=True)
class BenchmarkDeviceSignal:
    device_id: str
    is_online: bool


@dataclass(frozen=True)
class BenchmarkSegmentSignal:
    device_id: str
    market_id: str
    fixture_id: str
    is_online: bool
    phase: str
    period: str
    state: str


class ComparisonScenario(Enum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    HIGH_OVERLAP = "high-overlap"
    HIGH_CARDINALITY = "high-cardinality"
    MANY_SOURCE = "many-source"


@dataclass(frozen=True)
class ComparisonBenchmarkData:
    history: Any
    event_count: int
    device_count: int
    source_count: int


@dataclass(frozen=True)
class SegmentCohortBenchmarkData:
    history: Any
    event_count: int
    selection_count: int
    source_count: int


def _comparison_shape(scenario: ComparisonScenario) -> tuple[int, int, int]:
    if scenario is ComparisonScenario.SMALL:
        return 128, 16, 2
    if scenario is ComparisonScenario.MEDIUM:
        return 1_024, 128, 2
    if scenario is ComparisonScenario.LARGE:
        return 8_192, 512, 2
    if scenario is ComparisonScenario.HIGH_OVERLAP:
        return 2_048, 32, 2
    if scenario is ComparisonScenario.HIGH_CARDINALITY:
        return 4_096, 2_048, 2
    if scenario is ComparisonScenario.MANY_SOURCE:
        return 4_096, 256, 8
    return 1_024, 128, 2


def _online_state(
    scenario: ComparisonScenario,
    occurrence: int,
    source_index: int,
) -> bool:
    if scenario is ComparisonScenario.HIGH_OVERLAP:
        return occurrence % 8 >= 4
    if scenario is ComparisonScenario.HIGH_CARDINALITY:
        return occurrence % 2 == 1
    if scenario is ComparisonScenario.MANY_SOURCE:
        return (occurrence + source_index) % 2 == 1
    return occurrence % 2 == 1


def create_comparison_data(scenario: ComparisonScenario) -> ComparisonBenchmarkData:
    event_count, device_count, source_count = _comparison_shape(scenario)
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .track_window(
            "DeviceOffline",
            key=lambda signal: signal.device_id,
            is_active=lambda signal: not signal.is_online,
        )
    )
    occurrences = [0] * (device_count * source_count)
    for event_index in range(event_count):
        device_index = event_index % device_count
        source_index = (event_index // device_count) % source_count
        occurrence_index = (device_index * source_count) + source_index
        occurrence = occurrences[occurrence_index]
        occurrences[occurrence_index] = occurrence + 1
        is_online = _online_state(scenario, occurrence, source_index)
        pipeline.ingest(
            BenchmarkDeviceSignal(f"device-{device_index}", is_online),
            source=f"provider-{source_index}",
        )
    return ComparisonBenchmarkData(pipeline.history, event_count, device_count, source_count)


def create_segment_cohort_data() -> SegmentCohortBenchmarkData:
    event_count = 2_048
    selection_count = 128
    source_count = 4
    pipeline = (
        Spanfold.for_events()
        .record_windows()
        .window(
            "SelectionPriced",
            key=lambda signal: signal.device_id,
            is_active=lambda signal: not signal.is_online,
            segments=lambda signal: {
                "market": signal.market_id,
                "fixture": signal.fixture_id,
                "phase": signal.phase,
                "period": signal.period,
            },
            tags=lambda signal: {"state": signal.state},
        )
        .build()
    )
    for event_index in range(event_count):
        selection_index = event_index % selection_count
        source_index = (event_index // selection_count) % source_count
        occurrence = event_index // (selection_count * source_count)
        pipeline.ingest(
            BenchmarkSegmentSignal(
                f"selection-{selection_index}",
                f"market-{selection_index % 16}",
                f"fixture-{selection_index % 8}",
                (occurrence + source_index) % 2 == 1,
                "in-play" if occurrence % 2 == 0 else "pre-match",
                f"period-{occurrence % 4}",
                "active" if occurrence % 2 == 0 else "settled",
            ),
            source=f"provider-{source_index}",
        )
    return SegmentCohortBenchmarkData(pipeline.history, event_count, selection_count, source_count)


def comparison_benchmarks(data: ComparisonBenchmarkData) -> dict[str, Callable[[], Any]]:
    def base_builder() -> Any:
        return (
            data.history.compare("Benchmark Provider QA")
            .target("provider-0")
            .against("provider-1")
            .within(lambda scope: scope.window("DeviceOffline"))
        )

    result_for_export = (
        base_builder()
        .using(
            lambda comparators: comparators.overlap()
            .residual()
            .missing()
            .coverage()
            .gap()
            .symmetric_difference()
        )
        .run()
    )
    live_result_for_export = (
        base_builder()
        .using(lambda comparators: comparators.residual())
        .run_live(TemporalPoint.for_position(data.event_count + 1))
    )
    return {
        "prepare": lambda: base_builder().using(lambda c: c.overlap()).prepare(),
        "align": lambda: base_builder().using(lambda c: c.overlap()).prepare().align(),
        "run_overlap": lambda: base_builder().using(lambda c: c.overlap()).run(),
        "run_residual": lambda: base_builder().using(lambda c: c.residual()).run(),
        "run_coverage": lambda: base_builder().using(lambda c: c.coverage()).run(),
        "run_containment": lambda: base_builder().using(lambda c: c.containment()).run(),
        "run_lead_lag": lambda: base_builder()
        .using(
            lambda c: c.lead_lag(
                LeadLagTransition.START,
                TemporalAxis.PROCESSING_POSITION,
                10,
            )
        )
        .run(),
        "run_live_residual": lambda: base_builder()
        .using(lambda c: c.residual())
        .run_live(TemporalPoint.for_position(data.event_count + 1)),
        "run_multi_comparator": lambda: base_builder()
        .using(
            lambda c: c.overlap()
            .residual()
            .missing()
            .coverage()
            .gap()
            .symmetric_difference()
        )
        .run(),
        "export_json": result_for_export.export_json,
        "export_markdown": result_for_export.export_markdown,
        "export_live_json": live_result_for_export.export_json,
    }


def ingestion_benchmarks(event_count: int) -> dict[str, Callable[[], Any]]:
    def ingest() -> Any:
        pipeline = (
            Spanfold.for_events()
            .record_windows()
            .track_window(
                "DeviceOffline",
                key=lambda signal: signal.device_id,
                is_active=lambda signal: not signal.is_online,
            )
        )
        for index in range(event_count):
            pipeline.ingest(
                BenchmarkDeviceSignal(f"device-{index % 128}", index % 2 == 1),
                source=f"provider-{index % 2}",
            )
        return pipeline.history

    return {f"ingest_{event_count}": ingest}


def segment_cohort_benchmarks(data: SegmentCohortBenchmarkData) -> dict[str, Callable[[], Any]]:
    def base_builder() -> Any:
        return (
            data.history.compare("Segment Cohort QA")
            .target("provider-0")
            .against_cohort(
                "cohort",
                sources=("provider-1", "provider-2", "provider-3"),
                activity=CohortActivity.any(),
            )
            .within(window_name="SelectionPriced", segments={"phase": "in-play"})
        )

    return {
        "segment_cohort_residual": lambda: base_builder()
        .using(lambda c: c.residual())
        .run(),
        "segment_cohort_llm_context": lambda: base_builder()
        .using(lambda c: c.residual())
        .run()
        .export_llm_context(),
    }


def measure(name: str, benchmark: Callable[[], Any], iterations: int) -> tuple[str, float, float]:
    durations: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        benchmark()
        durations.append(time.perf_counter() - start)
    return name, statistics.mean(durations), min(durations)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true", help="Run one quick iteration.")
    parser.add_argument("--scenario", choices=[item.value for item in ComparisonScenario])
    args = parser.parse_args()

    iterations = 1 if args.smoke else 5
    scenarios = (
        [ComparisonScenario(args.scenario)]
        if args.scenario
        else [
            ComparisonScenario.SMALL,
            ComparisonScenario.MEDIUM,
            ComparisonScenario.HIGH_OVERLAP,
            ComparisonScenario.HIGH_CARDINALITY,
            ComparisonScenario.MANY_SOURCE,
        ]
    )
    rows: list[tuple[str, float, float]] = []
    for scenario in scenarios:
        data = create_comparison_data(scenario)
        for name, benchmark in comparison_benchmarks(data).items():
            rows.append(measure(f"{scenario.value}.{name}", benchmark, iterations))
    for event_count in (128, 1_024, 8_192):
        for name, benchmark in ingestion_benchmarks(event_count).items():
            rows.append(measure(name, benchmark, iterations))
    segment_data = create_segment_cohort_data()
    for name, benchmark in segment_cohort_benchmarks(segment_data).items():
        rows.append(measure(name, benchmark, iterations))

    print("benchmark,mean_ms,min_ms")
    for name, mean_seconds, min_seconds in rows:
        print(f"{name},{mean_seconds * 1000:.3f},{min_seconds * 1000:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
