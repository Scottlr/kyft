using Kyft; // Import Kyft's window, comparison, and export APIs.

var start = DateTimeOffset.Parse("2026-04-19T12:00:00Z"); // Use a fixed clock so sample output is deterministic.
var pipeline = Kyft.Kyft // Start building a Kyft event pipeline.
    .For<ServiceHealthSignal>() // Tell Kyft the event type that will be ingested.
    .RecordIntervals() // Keep opened and closed windows for later comparison.
    .WithEventTime(signal => signal.Timestamp) // Attach event timestamps to recorded windows.
    .Window( // Define the child window tracked from raw service-health events.
        "ServiceDegraded", // Name the state span that opens when a service is unhealthy.
        key: signal => signal.ServiceId, // Track each service independently.
        isActive: signal => !signal.IsHealthy) // Open the window while the service is unhealthy.
    .RollUp( // Define a parent window that summarizes child service state.
        "RegionImpacted", // Name the parent state span for a region with degraded services.
        key: signal => signal.RegionId, // Group service windows by region.
        isActive: children => children.AnyActive()) // Open the region window when any child is active.
    .Build(); // Materialize the configured pipeline.

Ingest("monitor-a", 0, isHealthy: true); // Provider A reports the service as healthy.
Ingest("monitor-b", 1, isHealthy: true); // Provider B reports the same healthy state slightly later.
Ingest("monitor-a", 2, isHealthy: false); // Provider A sees degradation first.
Ingest("monitor-b", 4, isHealthy: false); // Provider B sees the degradation later.
Ingest("monitor-b", 8, isHealthy: true); // Provider B sees recovery first.
Ingest("monitor-a", 10, isHealthy: true); // Provider A sees recovery later.

var historical = pipeline.Intervals // Use the recorded windows for historical analysis.
    .Compare("Monitor degradation QA") // Name the comparison for reports and exports.
    .Target("monitor-a", selector => selector.Source("monitor-a")) // Treat monitor A as the baseline.
    .Against("monitor-b", selector => selector.Source("monitor-b")) // Compare monitor B against that baseline.
    .Within(scope => scope.Window("ServiceDegraded")) // Limit the comparison to service degradation windows.
    .Using(comparators => comparators // Choose the metrics to emit.
        .Overlap() // Emit agreement segments.
        .Residual() // Emit monitor-A-only segments.
        .Coverage() // Emit coverage rows and summaries.
        .LeadLag(LeadLagTransition.Start, TemporalAxis.ProcessingPosition, toleranceMagnitude: 2)) // Measure start-time drift.
    .Run(); // Execute the historical comparison.

Console.WriteLine("Historical monitor QA"); // Print a readable section heading.
Console.WriteLine("overlap rows: " + historical.OverlapRows.Count); // Show agreement row count.
Console.WriteLine("monitor-a residual rows: " + historical.ResidualRows.Count); // Show baseline-only row count.
Console.WriteLine("coverage summaries: " + historical.CoverageSummaries.Count); // Show coverage summary count.
Console.WriteLine("lead/lag rows: " + historical.LeadLagRows.Count); // Show timing-drift row count.

Ingest("monitor-a", 14, isHealthy: false); // Start a new monitor-A degradation that is still open.

var live = pipeline.Intervals // Use the same recorded history for a live snapshot.
    .Compare("Live degradation QA") // Name the live comparison.
    .Target("monitor-a", selector => selector.Source("monitor-a")) // Keep monitor A as the target.
    .Against("monitor-b", selector => selector.Source("monitor-b")) // Compare monitor B against monitor A.
    .Within(scope => scope.Window("ServiceDegraded")) // Analyze service degradation windows only.
    .Using(comparators => comparators.Residual()) // Ask where monitor A is active without monitor B.
    .RunLive(TemporalPoint.ForPosition(20)); // Clip open windows to a deterministic live horizon.

Console.WriteLine(); // Separate historical and live output.
Console.WriteLine("Live monitor QA"); // Print a live-section heading.
Console.WriteLine("residual rows: " + live.ResidualRows.Count); // Show live baseline-only row count.
Console.WriteLine("provisional rows: " + live.ProvisionalRowFinalities().Count); // Show rows depending on open windows.
Console.WriteLine(); // Separate the summary from the explain output.
Console.WriteLine(live.ExportMarkdown()); // Export deterministic Markdown for review or agent handoff.

void Ingest(string provider, int minuteOffset, bool isHealthy) // Local helper for compact sample events.
{
    pipeline.Ingest( // Send one event through the pipeline.
        new ServiceHealthSignal( // Create a service-health event.
            ClusterId: "cluster-101", // Put the service in a stable cluster.
            RegionId: "eu-west", // Put the service in a stable region.
            ServiceId: "checkout-api", // Track one service key in the sample.
            Provider: provider, // Preserve the monitor identity on the event.
            IsHealthy: isHealthy, // Carry the state used by the window predicate.
            Timestamp: start.AddMinutes(minuteOffset)), // Derive deterministic event time.
        source: provider); // Attach the monitor identity to emitted windows.
}

public sealed record ServiceHealthSignal( // Define the event shape consumed by Kyft.
    string ClusterId, // Identifies the larger cluster.
    string RegionId, // Identifies the parent roll-up key.
    string ServiceId, // Identifies the child window key.
    string Provider, // Identifies the source monitor.
    bool IsHealthy, // Drives the active/inactive state.
    DateTimeOffset Timestamp); // Provides event time for interval history.
