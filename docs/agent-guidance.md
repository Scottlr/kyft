# Agent Guidance

Use Kyft comparisons as data-first artifacts. Prefer plans, diagnostics, rows,
and exports over ad hoc narrative.

Safe defaults:

- Use descriptor selectors. Prefer `Source`, `WindowName`, `Key`, `Partition`,
  `PositionRange`, and `TimeRange`.
- Avoid runtime selectors in plans that need JSON export.
- Keep processing-position analysis as the default unless event timestamps are
  recorded and required.
- Treat open windows as non-final unless an explicit horizon is supplied.
- Call `Validate()` before running dynamically assembled plans.
- Call `Explain()` when a result surprises you.
- Export JSON for CI, issue reports, and agent handoff.

Recommended staged workflow:

```csharp
var plan = pipeline.Intervals
    .Compare("Provider QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Using(comparators => comparators.Overlap().Residual().Coverage())
    .Build();

var diagnostics = plan.Validate();
var result = pipeline.Intervals.Compare("Provider QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Using(comparators => comparators.Overlap().Residual().Coverage())
    .Run();

var json = result.ExportJson();
```

When working in live scenarios, record the horizon in the plan:

```csharp
.Normalize(normalization => normalization.ClipOpenWindowsTo(TemporalPoint.ForPosition(horizon)))
```

That makes finality explicit in the resulting ranges and explain output.
