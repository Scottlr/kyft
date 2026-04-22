# Spanfold

Spanfold is a Python library for recording temporal state windows and comparing
them across sources, providers, lanes, or pipeline stages.

It helps answer:

- when a state was active
- which source observed it
- which source missed it
- where sources overlapped, diverged, or left gaps
- what was still open at an explicit point in time

This package is the Python package `spanfold`. It tracks the core C# Spanfold
library surface while keeping the Python API idiomatic, typed, and snake-case.

## Install for Development

```bash
python -m pip install -e ".[dev]"
```

## Getting Started

```python
from dataclasses import dataclass

from spanfold import Spanfold


@dataclass(frozen=True)
class DeviceStatus:
    device_id: str
    is_online: bool


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
pipeline.ingest(DeviceStatus("device-17", True), source="provider-a")

window = pipeline.history.closed_windows[0]
print(window.window_name, window.key, window.start_position, window.end_position)
```

Expected output:

```text
DeviceOffline device-17 1 2
```

## Comparing Sources

```python
result = (
    pipeline.history.compare("Provider QA")
    .target("provider-a")
    .against("provider-b")
    .within(window_name="DeviceOffline")
    .using("overlap", "residual", "missing", "coverage")
    .run()
)

print(result.to_markdown())
result.export_debug_html("artifacts/provider-qa.html")
result.export_llm_context("artifacts/provider-qa.llm.json")
```

Open windows are excluded from comparisons by default. To include live windows,
pass an explicit horizon:

```python
from spanfold import TemporalPoint

live = (
    pipeline.history.compare("Live QA")
    .target("provider-a")
    .against("provider-b")
    .within(window_name="DeviceOffline")
    .normalize(horizon=TemporalPoint.for_position(100))
    .using("overlap")
    .run()
)
```

Rows that depend on horizon-clipped open windows are marked `provisional`.

## Feature Parity

Implemented now:

- temporal points and half-open temporal ranges
- processing-position and event-time range support
- open and closed window recording
- source/lane and partition-aware runtime state
- segment and tag capture
- segment boundary close/reopen behavior
- source-window rollups with nested rollups and segment projection
- hierarchy comparison summaries
- direct history queries
- C#-compatible query aliases, snapshot records, and direct overlap/residual helpers
- pipeline metadata for configured window and rollup names
- annotations with known-at filtering
- lane liveness and silence helpers
- directional source matrix helper
- cohort comparison helpers with any/all/none/threshold activity rules
- comparison rows: overlap, residual, missing, coverage, gap, symmetric
  difference, containment, lead/lag, as-of
- horizon-based live/open-window comparison metadata
- comparison plans, prepared comparisons, deterministic alignment, comparator
  catalog, explain formats, and structured plan diagnostics
- deterministic JSON, JSON Lines, Markdown, self-contained debug HTML, and LLM
  context exports
- configured debug HTML and LLM context export options for `run()` and
  `run_live()`
- fixture CLI with `validate-plan`, `compare`, and `explain`
- testing helpers for fixtures, snapshots, assertions, and virtual clocks
- benchmark harness covering ingestion, comparison preparation/alignment,
  comparator execution, live residuals, exports, and segment/cohort scenarios

Run benchmark smoke coverage with:

```bash
python benchmarks/spanfold_benchmarks.py --smoke
```

## Terminology

Public examples use neutral domains such as monitoring, IoT, logistics, access
audit, distributed systems, and pipelines.
