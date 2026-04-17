# Kyft

Kyft is a composable C# library for stateful event windows and hierarchical
rollups.

The main model is state driven:

- define whether an entity is active
- feed events into a pipeline
- receive emissions when windows open or close
- roll child windows up into parent windows

## Example

```csharp
using Kyft;

var pipeline = Kyft
    .For<DeviceSignal>()
    .Window(
        "DeviceOffline",
        key: signal => signal.DeviceId,
        isActive: signal => !signal.IsOnline)
    .RollUp(
        "ZoneOutage",
        key: signal => signal.ZoneId,
        isActive: children => children.AllActive())
    .Build();

var opened = pipeline.Ingest(new DeviceSignal(
    DeviceId: "device-1",
    ZoneId: "zone-a",
    IsOnline: false));

foreach (var emission in opened.Emissions)
{
    Console.WriteLine($"{emission.WindowName} {emission.Kind} {emission.Key}");
}

public sealed record DeviceSignal(
    string DeviceId,
    string ZoneId,
    bool IsOnline);
```

For a pipeline with only one source window, use the shorter terminal helper:

```csharp
var pipeline = Kyft
    .For<DeviceSignal>()
    .TrackWindow(
        "DeviceOffline",
        signal => signal.DeviceId,
        signal => !signal.IsOnline);
```

Window-specific reactions live in optional window options:

```csharp
var pipeline = Kyft
    .For<DeviceSignal>()
    .TrackWindow(
        "DeviceOffline",
        signal => signal.DeviceId,
        signal => !signal.IsOnline,
        window => window
            .OnOpened(emission => Console.WriteLine($"Opened {emission.Key}"))
            .OnClosed(emission => Console.WriteLine($"Closed {emission.Key}")));
```

For reusable domain windows, define them as types:

```csharp
var pipeline = Kyft
    .For<DeviceSignal>()
    .Window<DeviceOffline>()
    .RollUp<ZoneOutage>()
    .Build();

public sealed class DeviceOffline : IWindowDefinition<DeviceSignal>
{
    public void Define(WindowDefinitionBuilder<DeviceSignal> window)
    {
        window
            .Key(signal => signal.DeviceId)
            .ActiveWhen(signal => !signal.IsOnline);
    }
}

public sealed class ZoneOutage : IRollUpDefinition<DeviceSignal>
{
    public void Define(RollUpDefinitionBuilder<DeviceSignal> rollUp)
    {
        rollUp
            .Key(signal => signal.ZoneId)
            .ActiveWhen(children => children.AllActive());
    }
}
```

## Intervals

Interval recording is opt-in with `RecordIntervals()`. Recorded intervals use
processing positions by default. If `WithEventTime(...)` is configured, closed
intervals also include event timestamps.

## Analysis

Closed interval history supports basic overlap queries and residual analysis for
source comparisons. These query helpers are intentionally separate from the main
pipeline builder surface.
