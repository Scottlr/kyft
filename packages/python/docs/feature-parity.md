# Feature Parity Map

## Implemented Immediately

- Package skeleton and tooling.
- Temporal points, axes, and half-open ranges.
- Open and closed window records.
- Source/lane and partition-aware ingestion.
- Segment and tag recording.
- Segment boundary close/reopen handling.
- Source-window rollups, nested rollups, and rollup segment projection.
- Parent/child hierarchy comparison summaries.
- Direct history query API.
- C#-compatible direct overlap and residual history helpers.
- Snapshot records and snapshot query materializers.
- Window group summaries by segment and tag.
- Pipeline metadata for configured window and rollup names.
- Known-at annotations.
- Liveness helpers for heartbeat, silence, and stale lanes.
- Directional source matrix helper.
- Cohort comparison helpers with any/all/none/threshold activity rules.
- Cohort evidence extension metadata and typed evidence access.
- Staged comparison: overlap, residual, missing, coverage, gap, symmetric
  difference, containment, lead/lag, as-of.
- Coverage summaries and row aggregation helpers.
- Horizon clipping for live/open windows with provisional finality.
- Row finality snapshots and deterministic changelog replay helpers.
- JSON, summary-first JSON Lines, Markdown, explain text, and self-contained
  debug HTML exports.
- Testing helpers for fixtures, snapshots, assertions, and virtual clocks.
- Comparison-level known-at filtering for processing-position availability.
- Basic runtime diagnostics for as-of future-leakage risk.
- Event-time comparison mode with missing timestamp diagnostics.
- Duplicate-window rejection and adjacent-window coalescing normalization.
- Full normalization policy object mirroring the C# plan surface.
- `run_live()` live comparison shortcut for explicit horizon clipping.
- Structured diagnostics with warning/error severity and strict-mode promotion.
- JSON export includes structured diagnostics and row finality snapshots.
- Extension descriptor and metadata surface.
- Segment cohort safety and known-at cohort regression coverage.

## Implement Later

- Benchmarks.

## Intentionally Different in Python

- Snake-case method and attribute names.
- Dataclass result models rather than C# records.
- Direct callables for key, active predicate, segments, and tags.
- Export methods return strings and optionally write paths.

## Not Yet Understood

- None currently tracked.
