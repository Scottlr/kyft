# Kyft

Kyft is a C# library for recording temporal state windows and comparing them
across sources, providers, or pipeline stages.

It is for systems where the important question is not only "what is the latest
value?", but:

- when was this state active?
- who observed it?
- who missed it?
- where did sources overlap, diverge, lag, or leave gaps?
- what was knowable at a specific point in time?

Kyft turns event predicates into recorded windows, then gives you a staged
comparison API for auditing those windows.

## Concrete Use Cases

- Compare two monitoring providers and find where one reported an outage that
  the other missed.
- Audit a detector or model decision using only the windows that were knowable
  at the decision point.
- Run live analysis over still-open windows and separate final rows from
  provisional rows.

## What Category Is This?

Kyft sits between event processing and analytics.

It is not a general stream processor, metrics library, or database abstraction.
It records the periods where a state was active, then compares those periods as
first-class temporal data.

The core flow is:

1. Define when a keyed state is active.
2. Ingest events.
3. Record open and closed windows.
4. Compare windows across sources or stages.
5. Export rows, summaries, diagnostics, and explanations.

## Install

```bash
dotnet add package Kyft # Add the core state-window recording and comparison package.
dotnet add package Kyft.Testing # Add optional helpers for fixtures, snapshots, and assertions.
```

`Kyft.Testing` is optional. It is useful in consumer test suites when you want
small comparison fixtures without running a full pipeline.

## First Example: Record Windows

Define the event shape, the key that owns state, and the predicate that says
when the window is active.

```csharp
using Kyft; // Import Kyft's pipeline and comparison APIs.

var pipeline = Kyft // Start a Kyft pipeline definition.
    .For<DeviceSignal>() // Configure the event type that will be ingested.
    .RecordIntervals() // Store opened and closed windows for comparison.
    .TrackWindow( // Define one state-driven window.
        "DeviceOffline", // Name the state span that will be recorded.
        key: signal => signal.DeviceId, // Track each device independently.
        isActive: signal => !signal.IsOnline); // Open the window while the device is offline.

pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false), source: "provider-a"); // Open provider A's window.
pipeline.Ingest(new DeviceSignal("device-1", IsOnline: true), source: "provider-a"); // Close provider A's window.
pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false), source: "provider-b"); // Open provider B's window.
pipeline.Ingest(new DeviceSignal("device-1", IsOnline: true), source: "provider-b"); // Close provider B's window.

public sealed record DeviceSignal(string DeviceId, bool IsOnline); // Define the event shape.
```

`RecordIntervals()` is the important switch. It keeps the temporal evidence that
later comparisons, exports, and tests operate on.

## Compare Recorded Windows

Comparison is staged in the order an analyst usually asks the question:
baseline, comparison source, scope, normalization, metric.

```csharp
var result = pipeline.Intervals // Use the recorded window history.
    .Compare("Provider QA") // Name the comparison for reports and exports.
    .Target("provider-a", selector => selector.Source("provider-a")) // Treat provider A as the baseline.
    .Against("provider-b", selector => selector.Source("provider-b")) // Compare provider B against it.
    .Within(scope => scope.Window("DeviceOffline")) // Limit the question to one window family.
    .Using(comparators => comparators // Choose the row sets to emit.
        .Overlap() // Emit rows where both sources agreed the state was active.
        .Residual() // Emit provider-A-only rows.
        .Missing() // Emit provider-B-only rows.
        .Coverage()) // Emit magnitude and coverage summaries.
    .Run(); // Execute the historical comparison.

foreach (var row in result.ResidualRows) // Inspect target-only rows.
{
    Console.WriteLine($"{row.WindowName} {row.Key} missed from {row.Range.Start} to {row.Range.End}"); // Print each missed segment.
}
```

The result is structured data:

- diagnostics
- selected, excluded, and normalized windows
- aligned temporal segments
- comparator rows
- summaries
- finality metadata
- deterministic JSON, JSON Lines, and Markdown exports

## What Makes Kyft Different?

Kyft's main primitive is not an event, metric, or table row. It is a recorded
state window with comparison semantics.

That gives you:

- explicit open and closed windows
- source-aware comparison
- half-open temporal ranges
- overlap, residual, missing, coverage, gap, containment, lead/lag, and as-of
  comparators
- known-at filtering for point-in-time safety
- live horizons for ongoing windows
- deterministic explanations and exports

## Why Not Just X?

### Event Sourcing

Event sourcing is good for storing durable facts and rebuilding state. Kyft is
for analyzing the periods where state was active after those facts have been
interpreted.

Use event sourcing to preserve the facts. Use Kyft when you need to compare
"provider A thought this device was offline from 10 to 20" against "provider B
thought it was offline from 12 to 18".

### Stream Processors

Stream processors are good for online computation, routing, enrichment, and
continuous aggregation. Kyft is smaller and more focused: it records state
windows and compares them.

Use a stream processor to run a distributed pipeline. Use Kyft when your C#
code needs an inspectable temporal comparison artifact.

### Observability And Metrics Tools

Metrics tools are good for dashboards, counters, histograms, and alerts. They
usually compress time into aggregates.

Kyft keeps the individual windows and emits comparison rows. That matters when
you need to answer "which exact interval was missed?" rather than "what was the
average error rate?"

### Storing Intervals Directly In A Database

A database can store intervals, but it will not give you Kyft's comparison
model by itself: staged plans, source selectors, normalization, live finality,
known-at filtering, diagnostics, and deterministic exports.

Use the database for persistence. Use Kyft to define and execute the temporal
comparison logic.

## Comparator Families

- `Overlap()` shows agreement.
- `Residual()` shows target-only duration.
- `Missing()` shows comparison-only duration.
- `Coverage()` shows magnitude and coverage summaries.
- `Gap()` shows empty spaces inside an observed scope.
- `SymmetricDifference()` shows disagreement in both directions.
- `Containment()` checks whether one state stays inside another state.
- `LeadLag(...)` measures transition timing drift.
- `AsOf(...)` performs point-in-time lookup without future leakage.

See [Comparator reference](docs/comparator-reference.md) for row shapes and
details.

## Live Windows

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

foreach (var finality in live.ProvisionalRowFinalities()) // Inspect rows that may change later.
{
    Console.WriteLine($"{finality.RowId} is provisional: {finality.Reason}"); // Explain why the row is provisional.
}
```

The same plan converges with batch execution when all source windows close.

## Known-At Safety

Use `KnownAtPosition(...)` when a backtest, replay, or audit must only use
windows that were available at a decision point.

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

## Testing And Fixtures

Use `Kyft.Testing` when a test needs a compact comparison history.

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
coverage, lead/lag, live open windows, and Markdown export.

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
