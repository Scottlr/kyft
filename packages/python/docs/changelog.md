# Changelog

## 0.1.0 - Initial Python slice

- Added cohort evidence extension metadata with typed evidence access.
- Added extension descriptor and comparison extension metadata exports.
- Added coverage summaries and row aggregation helpers.
- Added window group summaries by segment and tag.
- Added full normalization policy object parity with the C# plan surface.
- Added comparison result query helpers for diagnostics and row finality.
- Changed JSON Lines export to stream a summary artifact before row artifacts.
- Added structured diagnostics and row finality snapshots to JSON exports.
- Added structured comparison diagnostics and strict-mode warning promotion.
- Added `run_live()` for first-class horizon-clipped live comparisons.
- Added duplicate-window rejection and adjacent-window coalescing normalization options.
- Added event-time comparison diagnostics for missing timestamps and tests for timestamp-based overlap.
- Added comparison-level known-at filtering, known-at/evaluation-horizon export metadata, and as-of future-leakage diagnostics.
- Added row finality snapshots and changelog replay helpers for provisional, revised, and retracted comparison rows.
- Added cohort comparison helpers with any/all/none/threshold activity rules.
- Added `spanfold.testing` fixture, snapshot, assertion, and virtual clock helpers.
- Added parent/child hierarchy comparison summaries.
- Added source-window rollups, nested rollups, and rollup segment projection.
- Added directional source matrix helpers.
- Added lane liveness/silence helpers that can feed normal window recording.
- Added as-of comparator rows with previous, next, and nearest matching.
- Added lead/lag comparator rows and summaries.
- Added gap, symmetric-difference, and containment comparison rows.
- Added modern Python package skeleton with `src/spanfold`, pytest, ruff, and mypy configuration.
- Implemented temporal points and half-open temporal ranges.
- Implemented source and partition-aware window recording.
- Added direct history queries, annotations, segments, and tags.
- Added overlap, residual, missing, and coverage comparison rows.
- Added JSON, JSON Lines, Markdown, and debug HTML exports.
- Added focused tests for the initial parity slice.

## Deferred parity

- Benchmarks and complete docs site.
