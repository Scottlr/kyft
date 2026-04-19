# Kyft

Kyft is a C# library for recording stateful event windows and comparing those
windows across sources, providers, or pipeline stages.

It is useful when the important question is not just "what is the latest value?"
but "when was this state active, who saw it, who missed it, and what was known at
the time?"

## Install

```bash
dotnet add package Kyft # Add the core event-windowing and comparison package.
dotnet add package Kyft.Testing # Add optional helpers for tests and fixtures.
```

`Kyft.Testing` is optional. It contains fixture builders, snapshot helpers,
virtual horizons, and comparison assertions for consumer test suites.

## Track Windows

Define the event shape, the key that owns state, and the predicate that says
when the window is active.

```csharp
using Kyft; // Import Kyft's pipeline and window APIs.

var pipeline = Kyft // Start a Kyft pipeline definition.
    .For<DeviceSignal>() // Configure the event type that will be ingested.
    .RecordIntervals() // Store opened and closed windows for later analysis.
    .TrackWindow( // Define a single state-driven window.
        "DeviceOffline", // Name the state span that will be recorded.
        key: signal => signal.DeviceId, // Track each device independently.
        isActive: signal => !signal.IsOnline); // Open the window while the device is offline.

pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false), source: "provider-a"); // Open the window.
pipeline.Ingest(new DeviceSignal("device-1", IsOnline: true), source: "provider-a"); // Close the window.

public sealed record DeviceSignal(string DeviceId, bool IsOnline); // Define the event shape.
```

`RecordIntervals()` keeps the opened and closed windows so they can be queried,
compared, exported, and tested.

## Compare Providers

Comparison is staged in the same order analysts usually ask the question:
baseline, comparison source, scope, normalization, metric.

```csharp
var result = pipeline.Intervals // Use the recorded window history.
    .Compare("Provider QA") // Name the comparison for reports and exports.
    .Target("provider-a", selector => selector.Source("provider-a")) // Treat provider A as the baseline.
    .Against("provider-b", selector => selector.Source("provider-b")) // Compare provider B against it.
    .Within(scope => scope.Window("DeviceOffline")) // Limit the scope to one window family.
    .Using(comparators => comparators // Pick the row sets to produce.
        .Overlap() // Emit rows where both providers agreed.
        .Residual() // Emit provider-A-only rows.
        .Coverage()) // Emit coverage rows and summaries.
    .Run(); // Execute the historical comparison.

foreach (var row in result.ResidualRows) // Inspect target-only rows.
{
    Console.WriteLine($"{row.WindowName} {row.Key} missed from {row.Range.Start} to {row.Range.End}"); // Print each gap.
}
```

Useful comparator families include:

- `Overlap()` for agreement.
- `Residual()` for target-only duration.
- `Missing()` for comparison-only duration.
- `Coverage()` for magnitude and ratio summaries.
- `LeadLag(...)` for transition timing.
- `AsOf(...)` for point-in-time enrichment.
- `Containment()` for invariants such as child state inside parent state.

## Handle Live Windows

Historical comparisons reject open windows by default. Live comparisons require
an explicit horizon and mark rows that depend on open windows as provisional.

```csharp
var live = pipeline.Intervals // Use the current recorded history.
    .Compare("Live provider QA") // Name the live comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Keep provider A as baseline.
    .Against("provider-b", selector => selector.Source("provider-b")) // Compare provider B against it.
    .Within(scope => scope.Window("DeviceOffline")) // Analyze one window family.
    .Using(comparators => comparators.Residual()) // Ask for baseline-only duration.
    .RunLive(TemporalPoint.ForPosition(100)); // Clip open windows to position 100.

foreach (var finality in live.ProvisionalRowFinalities()) // Inspect provisional rows.
{
    Console.WriteLine($"{finality.RowId} is provisional: {finality.Reason}"); // Explain why the row may change.
}
```

The same plan converges with batch execution when all windows close.

## Avoid Future Leakage

Use `KnownAtPosition(...)` when a backtest or audit must only use records that
were available at a decision point.

```csharp
var audit = pipeline.Intervals // Start from recorded windows.
    .Compare("Decision audit") // Name the point-in-time audit.
    .Target("detector", selector => selector.Source("detector")) // Treat detector output as the target.
    .Against("monitor", selector => selector.Source("monitor")) // Compare monitor history against it.
    .Within(scope => scope.Window("DeviceOffline")) // Audit the device-offline state.
    .Normalize(normalization => normalization.KnownAtPosition(42)) // Exclude records unavailable at position 42.
    .Using(comparators => comparators.AsOf( // Use a point-in-time lookup comparator.
        AsOfDirection.Previous, // Only match records known before the target point.
        TemporalAxis.ProcessingPosition, // Measure distance on processing positions.
        toleranceMagnitude: 10)) // Reject matches more than 10 positions away.
    .Run(); // Execute the leakage-safe audit.
```

Known-at is availability time. It is separate from event time.

## Test Fixtures

Use `Kyft.Testing` when a test needs a compact comparison history without
running a full event pipeline.

```csharp
using Kyft.Testing; // Import fixture and assertion helpers.

var history = new WindowHistoryFixtureBuilder() // Create a compact recorded history.
    .AddClosedWindow("DeviceOffline", "device-1", 1, 5, source: "provider-a") // Add provider A's window.
    .AddClosedWindow("DeviceOffline", "device-1", 3, 7, source: "provider-b") // Add provider B's window.
    .Build(); // Build a WindowIntervalHistory.

var result = history.Compare("Fixture QA") // Create a fixture comparison.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select provider A as target.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select provider B as comparison.
    .Within(scope => scope.Window("DeviceOffline")) // Limit the fixture to one window family.
    .Using(comparators => comparators.Overlap()) // Emit agreement rows.
    .Run(); // Execute the comparison.

KyftAssert.IsValid(result); // Fail the test if Kyft produced error diagnostics.
```

## Operations Example

The sample in `samples/Kyft.OperationsExample` compares service degradation
windows across monitoring providers. It demonstrates provider residuals,
coverage, lead/lag, live open windows, and markdown export.

```bash
dotnet run --project samples/Kyft.OperationsExample/Kyft.OperationsExample.csproj # Run the operations monitoring sample.
```

## More Detail

- [Comparison guide](docs/comparison-guide.md)
- [Comparator reference](docs/comparator-reference.md)
- [Live finality and changelog](docs/live-finality-and-changelog.md)
- [Fixture schema](docs/fixture-schema.md)
- [API shape review](docs/api-shape-review.md)
- [Performance notes](docs/performance-notes.md)
- [Agent guidance](docs/agent-guidance.md)
