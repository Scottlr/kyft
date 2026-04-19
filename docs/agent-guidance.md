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
var plan = pipeline.Intervals // Start from recorded window history.
    .Compare("Provider QA") // Name the comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Choose the baseline source.
    .Against("provider-b", selector => selector.Source("provider-b")) // Choose the comparison source.
    .Within(scope => scope.Window("DeviceOffline")) // Limit analysis to one window family.
    .Using(comparators => comparators.Overlap().Residual().Coverage()) // Request agreement, residual, and coverage.
    .Build(); // Build inspectable plan data without executing.

var diagnostics = plan.Validate(); // Check structural issues before execution.
var result = pipeline.Intervals.Compare("Provider QA") // Build the executable comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select the target again for execution.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select the comparison source again.
    .Within(scope => scope.Window("DeviceOffline")) // Use the same scope as the reviewed plan.
    .Using(comparators => comparators.Overlap().Residual().Coverage()) // Use the same comparator set.
    .Run(); // Execute and materialize rows.

var json = result.ExportJson(); // Export deterministic JSON for handoff.
```

When working in live scenarios, record the horizon in the plan:

```csharp
.Normalize(normalization => normalization.ClipOpenWindowsTo(TemporalPoint.ForPosition(horizon))) // Bound open windows explicitly.
```

That makes finality explicit in the resulting ranges and explain output.
