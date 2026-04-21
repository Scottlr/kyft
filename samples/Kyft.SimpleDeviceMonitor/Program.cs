using Kyft; // Import Kyft's pipeline, query, and comparison APIs.

var start = DateTimeOffset.Parse("2026-04-21T09:00:00Z"); // Use a fixed clock so output is deterministic.
var pipeline = Kyft.Kyft // Start a Kyft pipeline definition.
    .For<DeviceSignal>() // Consume device health signals.
    .RecordIntervals() // Record opened and closed windows.
    .WithEventTime(signal => signal.Timestamp) // Attach event time to recorded windows.
    .TrackWindow( // Define one state-driven window.
        "DeviceOffline", // Name the recorded state.
        key: signal => signal.DeviceId, // Track each device independently.
        isActive: signal => !signal.IsOnline); // Keep the window open while the device is offline.

Ingest("agent-a", 0, true); // Agent A starts healthy.
Ingest("agent-b", 0, true); // Agent B starts healthy.
Ingest("agent-a", 5, false); // Agent A sees the outage first.
Ingest("agent-b", 7, false); // Agent B sees the same outage later.
Ingest("agent-b", 12, true); // Agent B sees recovery first.
Ingest("agent-a", 15, true); // Agent A sees recovery later.
Ingest("agent-a", 20, false); // Agent A opens a second outage.

var closed = pipeline.Intervals.Query() // Start a direct history query.
    .Window("DeviceOffline") // Read only device-offline windows.
    .Lane("agent-a") // Read one source lane.
    .ClosedWindows(); // Materialize closed windows for that lane.

var live = pipeline.Intervals.Query() // Start another direct history query.
    .Window("DeviceOffline") // Read only device-offline windows.
    .Lane("agent-a") // Read one source lane.
    .OpenWindowsAt(TemporalPoint.ForPosition(8)); // Ask what was open at position 8.

var comparison = pipeline.Intervals // Start a source-aware comparison.
    .Compare("Simple monitor comparison") // Name the comparison.
    .Target("agent-a", selector => selector.Source("agent-a")) // Treat agent A as the target.
    .Against("agent-b", selector => selector.Source("agent-b")) // Compare agent B against it.
    .Within(scope => scope.Window("DeviceOffline")) // Scope the comparison to offline windows.
    .Using(comparators => comparators.Overlap().Residual().Missing().Coverage()) // Emit agreement, disagreement, and coverage rows.
    .RunLive(TemporalPoint.ForPosition(25)); // Clip the open second outage to a live horizon.

Console.WriteLine("Simple device monitor"); // Print the sample title.
Console.WriteLine("agent-a closed windows: " + closed.Count); // Show direct history output.
Console.WriteLine("agent-a windows open at position 8: " + live.Count); // Show horizon query output.
Console.WriteLine("overlap rows: " + comparison.OverlapRows.Count); // Show agreement row count.
Console.WriteLine("agent-a-only rows: " + comparison.ResidualRows.Count); // Show target-only row count.
Console.WriteLine("agent-b-only rows: " + comparison.MissingRows.Count); // Show comparison-only row count.
Console.WriteLine("provisional rows: " + comparison.ProvisionalRowFinalities().Count); // Show live rows that may change.

void Ingest(string agent, int minute, bool isOnline) // Keep sample event creation compact.
{
    pipeline.Ingest( // Send one signal into Kyft.
        new DeviceSignal( // Create the event payload.
            "device-17", // Track one logical device.
            isOnline, // Carry the online state.
            start.AddMinutes(minute)), // Use deterministic event time.
        source: agent); // Store the reporting agent as the lane/source.
}

public sealed record DeviceSignal( // Define the event type consumed by the sample.
    string DeviceId, // Identifies the monitored device.
    bool IsOnline, // Drives the offline window predicate.
    DateTimeOffset Timestamp); // Provides event-time bounds.
