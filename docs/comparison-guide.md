# Comparison Guide

Kyft comparisons answer staged questions over recorded windows:

- target: the source or stage to treat as the baseline
- against: one or more sources or stages to compare with the target
- scope: the window family and temporal axis to analyse
- normalization: how recorded windows become comparable temporal ranges
- comparators: the row sets to emit

The API keeps these stages visible:

```csharp
var result = pipeline.Intervals // Start from the recorded window history.
    .Compare("Provider QA") // Name the comparison for exports and diagnostics.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select the baseline source.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select the comparison source.
    .Within(scope => scope.Window("DeviceOffline")) // Limit analysis to one window family.
    .Using(comparators => comparators.Overlap().Residual().Coverage()) // Request agreement, residual, and coverage rows.
    .Run(); // Execute the comparison.
```

Use descriptor selectors such as `Source`, `WindowName`, `Key`, `Partition`,
`PositionRange`, and `TimeRange` whenever a plan needs to be exported or
reviewed by tooling. Runtime selectors are useful locally, but deterministic
JSON export rejects them because a delegate cannot be represented as portable
data.

## Temporal Model

Processing-position comparisons are the default. Positions are assigned during
ingestion and are deterministic for a replay of the same event order.

Event-time comparisons require timestamps:

```csharp
var result = pipeline.Intervals // Start from recorded windows.
    .Compare("Event-time QA") // Name the event-time comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select provider A as target.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select provider B as comparison.
    .Within(scope => scope.Window("DeviceOffline")) // Scope to one window family.
    .Normalize(normalization => normalization.OnEventTime()) // Compare on event timestamps instead of positions.
    .Using(comparators => comparators.Overlap()) // Emit rows where both sources were active.
    .Run(); // Execute the comparison.
```

Scope and normalization must use the same temporal axis. Kyft rejects mixed-axis
plans because processing positions and timestamps cannot be compared without an
explicit mapping.

Ranges are half-open: `[start, end)`. The start is included and the end is
excluded. Touching windows create adjacent segments, not overlaps.

## Open Windows

Open windows are rejected by default in historical comparison. This avoids
silently treating an ongoing window as final.

For live or horizon-based analysis, choose an explicit end:

```csharp
var prepared = pipeline.Intervals // Start from recorded windows.
    .Compare("Live QA") // Name the live preparation.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select the baseline source.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select the comparison source.
    .Within(scope => scope.Window("DeviceOffline")) // Analyze one window family.
    .Normalize(normalization => normalization.ClipOpenWindowsTo(TemporalPoint.ForPosition(100))) // Bound open windows at position 100.
    .Using(comparators => comparators.Coverage()) // Request coverage metrics.
    .Prepare(); // Stop after selection and normalization for inspection.
```

The resulting range records that its end came from the horizon policy, not from
a closed source window.

## Known-At Safety

Known-at filtering answers "what records were available at this processing
position?" It is availability time, not event time. Closed windows become
available when their close position has been processed; open windows are
available from their start position.

```csharp
var prepared = pipeline.Intervals // Start from recorded windows.
    .Compare("Decision audit") // Name the point-in-time audit.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select the target source.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select the comparison source.
    .Within(scope => scope.Window("DeviceOffline")) // Audit one window family.
    .Normalize(normalization => normalization.KnownAtPosition(42)) // Exclude windows unavailable at position 42.
    .Using(comparators => comparators.Overlap()) // Emit agreement rows.
    .Prepare(); // Inspect prepared data and diagnostics without comparator execution.
```

Records unavailable at the known-at point are excluded with diagnostics so
backtests and decision audits do not silently leak future information.

## Live Snapshots

Use `RunLive(horizon)` when the last window may still be open. Kyft clips open
windows to the supplied evaluation horizon and marks rows that depend on those
windows as provisional. Closed-window rows remain final, and the same comparison
converges with batch execution once all windows close.

```csharp
var result = pipeline.Intervals // Start from current recorded history.
    .Compare("Live QA") // Name the live comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select provider A as target.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select provider B as comparison.
    .Within(scope => scope.Window("DeviceOffline")) // Scope to device-offline windows.
    .Using(comparators => comparators.Residual()) // Emit target-only rows.
    .RunLive(TemporalPoint.ForPosition(100)); // Clip open windows to position 100.
```

The result carries `EvaluationHorizon` and row finality metadata so consumers can
separate current-state insight from final historical evidence.

## Live Revisions

Use `ComparisonChangelog.Create(previous.RowFinalities, current.RowFinalities)`
to audit how live row metadata changed between snapshots. Revised entries
supersede earlier row versions, and retracted entries remove rows that no longer
exist in the current snapshot. `ComparisonChangelog.Replay(...)` rebuilds the
active row-finality view from a prior snapshot plus its changelog entries.

For practical usage patterns, see
[Live finality and changelog](live-finality-and-changelog.md).

## Plan Criticism

Runtime criticism flags plans that are structurally valid but analytically risky:
runtime-only selectors, unrestricted scopes, point-in-time lookup without
known-at safety, open durations without a horizon, live clipping without a
horizon, and incompatible timestamp clocks. Non-strict execution carries these
diagnostics as warnings. Use `Strict()` when warnings should block alignment and
comparator rows.

## Extension Metadata

Domain packages can describe their own selectors, comparator declarations, and
metadata keys with `ComparisonExtensionBuilder`. Core Kyft stays domain-neutral;
extension descriptors document how a package attaches to plans, while result
`ExtensionMetadata` keeps compact domain metadata serializable and explainable.

## Inspectability

Use `Validate()` before execution when building plans dynamically. Use
`Prepare()` when you need to inspect selected, excluded, and normalized windows.
Use `Run()` when comparator rows are needed.

```csharp
var prepared = pipeline.Intervals // Start from recorded windows.
    .Compare("Provider QA") // Name the comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select the target source.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select the comparison source.
    .Within(scope => scope.Window("DeviceOffline")) // Limit the scope to one window family.
    .Using(comparators => comparators.Overlap()) // Request overlap rows.
    .Prepare(); // Materialize selected, excluded, and normalized windows.

var explanation = prepared.Explain(); // Render deterministic diagnostic text.
```

`Explain()` returns deterministic diagnostic text. It is not generated prose.
`ExportJson()` returns deterministic JSON for CI artifacts, issue reports, and
agent workflows. `ExportJsonLines()` streams result rows for larger outputs.
`ExportDebugHtml()` writes a standalone visual artifact for browser-based
debugging.

```csharp
var result = pipeline.Intervals // Start from recorded windows.
    .Compare("Provider QA") // Name the comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select the target source.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select the comparison source.
    .Within(scope => scope.Window("DeviceOffline")) // Limit the scope to one window family.
    .Using(comparators => comparators.Overlap().Residual().Missing()) // Request visible agreement and divergence rows.
    .Run(); // Execute the comparison.

result.ExportDebugHtml("artifacts/provider-qa.html"); // Write a self-contained HTML graph for debugging.
```

When configuration should control whether a run writes a debug artifact, pass
options to `Run()` or `RunLive()`:

```csharp
var debugHtml = ComparisonDebugHtmlOptions.ToFile("artifacts/provider-qa.html"); // Enable a visual artifact for this run.
var resultWithDebug = pipeline.Intervals // Start from recorded windows.
    .Compare("Provider QA") // Name the comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select the target source.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select the comparison source.
    .Within(scope => scope.Window("DeviceOffline")) // Limit the scope to one window family.
    .Using(comparators => comparators.Overlap().Residual()) // Request agreement and target-only rows.
    .Run(debugHtml); // Execute and write the debug file.
```

Use debug HTML at workflow boundaries, test failure boundaries, notebook
boundaries, incident handoff boundaries, or agent handoff boundaries. Avoid
writing it on every ingestion event.

Use `ErrorDiagnostics()`, `WarningDiagnostics()`,
`ProvisionalRowFinalities()`, and `FinalRowFinalities()` when a consumer only
needs a filtered view of the result metadata.

## Performance Guidance

Plans are cheap. Preparation enumerates recorded history and normalization work.
Comparator execution materializes result rows. Avoid exporting or explaining
results in ingestion hot paths; build those artifacts at workflow boundaries,
test failure boundaries, notebook boundaries, or agent handoff points.

## Source Matrix

Use a source matrix when the same pairwise comparison needs to be read across
several sources:

```csharp
var matrix = pipeline.Intervals.CompareSources( // Build a directional source matrix.
    "Provider matrix", // Name the matrix for reports.
    "DeviceOffline", // Compare one window family.
    ["provider-a", "provider-b", "provider-c"]); // Include these sources as rows and columns.
```

Cells are directional. The row source is the target and the column source is
the comparison source. Diagonal cells are identity rows and do not run
comparators. Missing sources are still emitted as explicit cells so reports do
not silently drop an expected provider.

## Hierarchy Explanation

Use hierarchy comparison to explain parent rollup activity from child
contribution windows:

```csharp
var hierarchy = pipeline.Intervals.CompareHierarchy( // Explain parent activity from child windows.
    "Region explanation", // Name the hierarchy report.
    parentWindowName: "RegionImpacted", // Select the parent roll-up window.
    childWindowName: "DeviceOffline"); // Select the child contribution window.
```

Rows are emitted as explained parent activity, unexplained parent duration, or
orphan child duration. Current lineage is inferred from matching source and
partition for the supplied parent and child window names. Parent and child keys
may differ because rollups often aggregate several child keys into one parent
key.
