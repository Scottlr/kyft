# Live Finality And Changelog

Kyft separates historical comparison from live comparison.

Historical comparison uses `Run()` and rejects open windows by default. Live
comparison uses `RunLive(horizon)` and clips open windows to an explicit
evaluation horizon.

## Finality

Rows emitted by `RunLive(...)` carry finality metadata:

- `Final` means the row only depends on closed source windows.
- `Provisional` means the row depends on at least one open source window clipped
  to the evaluation horizon.

Use helpers when a report or agent only needs one view:

```csharp
var live = history.Compare("Live QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("SelectionUnavailable"))
    .Using(comparators => comparators.Residual())
    .RunLive(TemporalPoint.ForPosition(100));

if (live.HasProvisionalRows())
{
    foreach (var finality in live.ProvisionalRowFinalities())
    {
        Console.WriteLine($"{finality.RowId}: {finality.Reason}");
    }
}
```

## Changelog

`ComparisonChangelog.Create(previous, current)` compares row-finality metadata
between snapshots. It emits deterministic entries for:

- new row metadata
- revised metadata
- retracted metadata

`ComparisonChangelog.Replay(previous, entries)` rebuilds the current row-finality
view from a prior view plus its changelog entries.

This is useful for dashboards, agents, notebooks, and audit logs that need to
explain why a live answer changed.

## Fixture Live Horizon

CLI fixture plans can include `liveHorizonPosition`:

```json
{
  "plan": {
    "name": "Live Provider QA",
    "targetSource": "provider-a",
    "againstSources": [ "provider-b" ],
    "scopeWindow": "SelectionUnavailable",
    "comparators": [ "residual" ],
    "strict": false,
    "liveHorizonPosition": 100
  }
}
```

When this value is present, the fixture runner executes the comparison as a live
snapshot. Windows with `"endPosition": null` are treated as open windows.
