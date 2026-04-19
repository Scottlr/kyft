# Comparison Guide

Kyft comparisons answer staged questions over recorded windows:

- target: the source or stage to treat as the baseline
- against: one or more sources or stages to compare with the target
- scope: the window family and temporal axis to analyse
- normalization: how recorded windows become comparable temporal ranges
- comparators: the row sets to emit

The API keeps these stages visible:

```csharp
var result = pipeline.Intervals
    .Compare("Provider QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Using(comparators => comparators.Overlap().Residual().Coverage())
    .Run();
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
var result = pipeline.Intervals
    .Compare("Event-time QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Normalize(normalization => normalization.OnEventTime())
    .Using(comparators => comparators.Overlap())
    .Run();
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
var prepared = pipeline.Intervals
    .Compare("Live QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Normalize(normalization => normalization.ClipOpenWindowsTo(TemporalPoint.ForPosition(100)))
    .Using(comparators => comparators.Coverage())
    .Prepare();
```

The resulting range records that its end came from the horizon policy, not from
a closed source window.

## Known-At Safety

Known-at filtering answers "what records were available at this processing
position?" It is availability time, not event time. Closed windows become
available when their close position has been processed; open windows are
available from their start position.

```csharp
var prepared = pipeline.Intervals
    .Compare("Decision audit")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Normalize(normalization => normalization.KnownAtPosition(42))
    .Using(comparators => comparators.Overlap())
    .Prepare();
```

Records unavailable at the known-at point are excluded with diagnostics so
backtests and decision audits do not silently leak future information.

## Live Snapshots

Use `RunLive(horizon)` when the last window may still be open. Kyft clips open
windows to the supplied evaluation horizon and marks rows that depend on those
windows as provisional. Closed-window rows remain final, and the same comparison
converges with batch execution once all windows close.

```csharp
var result = pipeline.Intervals
    .Compare("Live QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Using(comparators => comparators.Residual())
    .RunLive(TemporalPoint.ForPosition(100));
```

The result carries `EvaluationHorizon` and row finality metadata so consumers can
separate current-state insight from final historical evidence.

## Inspectability

Use `Validate()` before execution when building plans dynamically. Use
`Prepare()` when you need to inspect selected, excluded, and normalized windows.
Use `Run()` when comparator rows are needed.

```csharp
var prepared = pipeline.Intervals
    .Compare("Provider QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Using(comparators => comparators.Overlap())
    .Prepare();

var explanation = prepared.Explain();
```

`Explain()` returns deterministic diagnostic text. It is not generated prose.
`ExportJson()` returns deterministic JSON for CI artifacts, issue reports, and
agent workflows. `ExportJsonLines()` streams result rows for larger outputs.

## Performance Guidance

Plans are cheap. Preparation enumerates recorded history and normalization work.
Comparator execution materializes result rows. Avoid exporting or explaining
results in ingestion hot paths; build those artifacts at workflow boundaries,
test failure boundaries, notebook boundaries, or agent handoff points.

## Source Matrix

Use a source matrix when the same pairwise comparison needs to be read across
several sources:

```csharp
var matrix = pipeline.Intervals.CompareSources(
    "Provider matrix",
    "DeviceOffline",
    ["provider-a", "provider-b", "provider-c"]);
```

Cells are directional. The row source is the target and the column source is
the comparison source. Diagonal cells are identity rows and do not run
comparators. Missing sources are still emitted as explicit cells so reports do
not silently drop an expected provider.

## Hierarchy Explanation

Use hierarchy comparison to explain parent rollup activity from child
contribution windows:

```csharp
var hierarchy = pipeline.Intervals.CompareHierarchy(
    "Market explanation",
    parentWindowName: "MarketSuspension",
    childWindowName: "SelectionSuspension");
```

Rows are emitted as explained parent activity, unexplained parent duration, or
orphan child duration. Current lineage is inferred from matching source and
partition for the supplied parent and child window names. Parent and child keys
may differ because rollups often aggregate several child keys into one parent
key.
