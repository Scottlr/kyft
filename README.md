# Kyft

Kyft is a C# library for recording stateful event windows and comparing those
windows across sources, providers, or pipeline stages.

It is useful when the important question is not just "what is the latest value?"
but "when was this state active, who saw it, who missed it, and what was known at
the time?"

## Install

```bash
dotnet add package Kyft
dotnet add package Kyft.Testing
```

`Kyft.Testing` is optional. It contains fixture builders, snapshot helpers,
virtual horizons, and comparison assertions for consumer test suites.

## Track Windows

Define the event shape, the key that owns state, and the predicate that says
when the window is active.

```csharp
using Kyft;

var pipeline = Kyft
    .For<DeviceSignal>()
    .RecordIntervals()
    .TrackWindow(
        "DeviceOffline",
        key: signal => signal.DeviceId,
        isActive: signal => !signal.IsOnline);

pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false), source: "provider-a");
pipeline.Ingest(new DeviceSignal("device-1", IsOnline: true), source: "provider-a");

public sealed record DeviceSignal(string DeviceId, bool IsOnline);
```

`RecordIntervals()` keeps the opened and closed windows so they can be queried,
compared, exported, and tested.

## Compare Providers

Comparison is staged in the same order analysts usually ask the question:
baseline, comparison source, scope, normalization, metric.

```csharp
var result = pipeline.Intervals
    .Compare("Provider QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Using(comparators => comparators
        .Overlap()
        .Residual()
        .Coverage())
    .Run();

foreach (var row in result.ResidualRows)
{
    Console.WriteLine($"{row.WindowName} {row.Key} missed from {row.Range.Start} to {row.Range.End}");
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
var live = pipeline.Intervals
    .Compare("Live provider QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Using(comparators => comparators.Residual())
    .RunLive(TemporalPoint.ForPosition(100));

foreach (var finality in live.ProvisionalRowFinalities())
{
    Console.WriteLine($"{finality.RowId} is provisional: {finality.Reason}");
}
```

The same plan converges with batch execution when all windows close.

## Avoid Future Leakage

Use `KnownAtPosition(...)` when a backtest or audit must only use records that
were available at a decision point.

```csharp
var audit = pipeline.Intervals
    .Compare("Decision audit")
    .Target("model", selector => selector.Source("model"))
    .Against("provider", selector => selector.Source("provider"))
    .Within(scope => scope.Window("SelectionUnavailable"))
    .Normalize(normalization => normalization.KnownAtPosition(42))
    .Using(comparators => comparators.AsOf(
        AsOfDirection.Previous,
        TemporalAxis.ProcessingPosition,
        toleranceMagnitude: 10))
    .Run();
```

Known-at is availability time. It is separate from event time.

## Test Fixtures

Use `Kyft.Testing` when a test needs a compact comparison history without
running a full event pipeline.

```csharp
using Kyft.Testing;

var history = new WindowHistoryFixtureBuilder()
    .AddClosedWindow("DeviceOffline", "device-1", 1, 5, source: "provider-a")
    .AddClosedWindow("DeviceOffline", "device-1", 3, 7, source: "provider-b")
    .Build();

var result = history.Compare("Fixture QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("DeviceOffline"))
    .Using(comparators => comparators.Overlap())
    .Run();

KyftAssert.IsValid(result);
```

## Bookmaker Example

The sample in `samples/Kyft.BookmakerExample` compares bookmaker market and
selection availability across providers. It demonstrates provider residuals,
coverage, lead/lag, live open windows, and markdown export.

```bash
dotnet run --project samples/Kyft.BookmakerExample/Kyft.BookmakerExample.csproj
```

## More Detail

- [Comparison guide](docs/comparison-guide.md)
- [Comparator reference](docs/comparator-reference.md)
- [Live finality and changelog](docs/live-finality-and-changelog.md)
- [Fixture schema](docs/fixture-schema.md)
- [API shape review](docs/api-shape-review.md)
- [Performance notes](docs/performance-notes.md)
- [Agent guidance](docs/agent-guidance.md)
