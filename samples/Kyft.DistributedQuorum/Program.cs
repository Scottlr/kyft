using Kyft; // Import Kyft's comparison and cohort APIs.

var start = DateTimeOffset.Parse("2026-04-21T12:00:00Z"); // Use deterministic event time.
var pipeline = Kyft.Kyft // Start a Kyft pipeline definition.
    .For<ReplicaState>() // Consume replica state updates.
    .RecordIntervals() // Record degraded-replica windows.
    .WithEventTime(state => state.Timestamp) // Attach event timestamps.
    .Window("ReplicaUnavailable", window => window // Define a replica availability window.
        .Key(state => state.ShardId) // Track each shard independently.
        .ActiveWhen(state => !state.IsAvailable) // Open while a replica reports unavailable.
        .Segment("region", segment => segment.Value(state => state.Region)) // Split by region.
        .Tag("replicaRole", state => state.Role)) // Attach role metadata.
    .Build(); // Build the pipeline.

Ingest("replica-a", 0, true, "primary", "region-1"); // Replica A starts available.
Ingest("replica-b", 0, true, "secondary", "region-1"); // Replica B starts available.
Ingest("replica-c", 0, true, "secondary", "region-2"); // Replica C starts available.
Ingest("replica-a", 10, false, "primary", "region-1"); // Replica A becomes unavailable.
Ingest("replica-b", 12, false, "secondary", "region-1"); // Replica B also becomes unavailable.
Ingest("replica-c", 15, false, "secondary", "region-2"); // Replica C becomes unavailable later.
Ingest("replica-b", 25, true, "secondary", "region-1"); // Replica B recovers.
Ingest("replica-a", 30, true, "primary", "region-1"); // Replica A recovers.
Ingest("replica-c", 35, true, "secondary", "region-2"); // Replica C recovers.

var quorumGap = pipeline.Intervals // Start a target-vs-cohort comparison.
    .Compare("Primary unavailable without quorum corroboration") // Name the comparison.
    .Target("primary", selector => selector.Source("replica-a")) // Treat the primary replica as target.
    .AgainstCohort("replica cohort", cohort => cohort // Compare against a derived cohort.
        .Sources("replica-b", "replica-c") // Include secondary replicas.
        .Activity(CohortActivity.AtLeast(2))) // Require both secondaries to be unavailable.
    .Within(scope => scope.Window("ReplicaUnavailable")) // Scope to replica-unavailable windows.
    .Using(comparators => comparators.Residual().Coverage()) // Emit primary-only time and coverage.
    .Run(); // Execute the cohort comparison.

var matrix = pipeline.Intervals.CompareSources( // Build a directional source matrix.
    "Replica source matrix", // Name the matrix.
    "ReplicaUnavailable", // Compare unavailable windows.
    ["replica-a", "replica-b", "replica-c"]); // Include all replica lanes.

Console.WriteLine("Distributed quorum analysis"); // Print the sample title.
Console.WriteLine("primary residual rows against cohort: " + quorumGap.ResidualRows.Count); // Show target-only rows.
Console.WriteLine("coverage summaries: " + quorumGap.CoverageSummaries.Count); // Show cohort coverage summaries.
Console.WriteLine("source-matrix cells: " + matrix.Cells.Count); // Show pairwise matrix size.

void Ingest(string source, int second, bool isAvailable, string role, string region) // Keep sample ingestion compact.
{
    pipeline.Ingest( // Send one replica update through Kyft.
        new ReplicaState( // Create the replica state event.
            "shard-42", // Track one logical shard.
            role, // Attach the replica role.
            region, // Attach region context.
            isAvailable, // Drive the unavailable predicate.
            start.AddSeconds(second)), // Use deterministic event time.
        source: source); // Store the replica identity as the source lane.
}

public sealed record ReplicaState( // Define the event type.
    string ShardId, // Identifies the replicated shard.
    string Role, // Describes the replica role.
    string Region, // Splits windows by region.
    bool IsAvailable, // Drives the unavailable predicate.
    DateTimeOffset Timestamp); // Provides event-time bounds.
