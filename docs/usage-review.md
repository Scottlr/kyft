# Usage Review

Kyft is easiest to understand when the public examples stay close to the
state-driven mental model:

- pick an event type
- define the key that owns state
- define when that key is active
- optionally roll child windows into parent windows
- ingest events and react to emissions

## What Feels Good

The small inline case is compact:

```csharp
var pipeline = Kyft
    .For<DeviceSignal>()
    .TrackWindow(
        "DeviceOffline",
        signal => signal.DeviceId,
        signal => !signal.IsOnline);
```

The reusable type path gives larger domains a place to put naming, key
selection, active-state logic, and callbacks:

```csharp
var pipeline = Kyft
    .For<DeviceSignal>()
    .Window<DeviceOffline>()
    .RollUp<ZoneOutage>()
    .Build();
```

This is a good split. Lambdas are still low-friction for simple windows, while
definition types keep complex windows from turning the pipeline declaration into
a long block of inline logic.

## Current Friction

The word `active` is still slightly abstract. In examples, the window name should
make the state obvious: `DeviceOffline`, `ZoneOutage`, `JobRunning`,
`MachineOverheated`.

Roll-up examples need at least two child events before `AllActive()` feels
intuitive. Single-child examples are valid, but they can make a roll-up look like
a duplicate of the child window.

Callbacks are in a reasonable place now: local reactions belong in window
options, while `.OnEmission(...)` remains useful as a global sink.

Window history now has a clearer primitive: `Chunk`. Open chunks and
closed chunks both stem from that shared type, and
`pipeline.Intervals.Chunks` gives future overlap, weighted-average, and hook
features a single object shape to build on.

## Example Direction

The public documentation now uses device telemetry instead of pricing,
selections, and markets. Device telemetry is a neutral example domain and maps
clearly onto Kyft concepts:

- `DeviceSignal` is the event
- `DeviceOffline` is the source window
- `ZoneOutage` is the roll-up

That framing makes Kyft read as a general event-windowing library rather than a
library tied to one betting-oriented use case.
