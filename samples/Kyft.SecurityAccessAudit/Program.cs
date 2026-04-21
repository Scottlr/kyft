using Kyft; // Import Kyft's temporal history APIs.

var start = DateTimeOffset.Parse("2026-04-21T11:00:00Z"); // Use deterministic event time.
var pipeline = Kyft.Kyft // Start a Kyft pipeline definition.
    .For<AccountRiskSignal>() // Consume account-risk signals.
    .RecordIntervals() // Record risk windows.
    .WithEventTime(signal => signal.Timestamp) // Attach event timestamps.
    .Window("AccountRiskElevated", window => window // Define a risk-state window.
        .Key(signal => signal.AccountId) // Track each account independently.
        .ActiveWhen(signal => signal.RiskScore >= 70) // Open while risk is elevated.
        .Segment("accessSurface", segment => segment.Value(signal => signal.AccessSurface)) // Split by access surface.
        .Tag("tenant", signal => signal.TenantId)) // Attach tenant metadata.
    .Build(); // Build the pipeline.

Ingest("risk-engine-a", 0, 10, "console"); // Engine A starts low risk.
Ingest("risk-engine-b", 0, 12, "console"); // Engine B starts low risk.
Ingest("risk-engine-a", 4, 85, "console"); // Engine A detects elevated risk.
Ingest("risk-engine-b", 6, 78, "console"); // Engine B detects elevated risk later.
Ingest("risk-engine-a", 12, 40, "console"); // Engine A clears the risk.
Ingest("risk-engine-b", 16, 35, "console"); // Engine B clears the risk later.

var elevated = pipeline.Intervals.Query() // Start a direct history query.
    .Window("AccountRiskElevated") // Scope to risk windows.
    .Lane("risk-engine-a") // Read one detector lane.
    .LatestWindow(); // Get the latest recorded window.

var annotation = pipeline.Intervals.Annotate( // Attach explanatory metadata after the fact.
    elevated!, // Annotate the risk window.
    "rootCause", // Name the annotation.
    "impossible-travel", // Store the explanation.
    TemporalPoint.ForPosition(7)); // Record when the explanation became known.

var safeAnnotations = pipeline.Intervals.AnnotationsKnownAt( // Read only point-in-time-safe annotations.
    elevated!, // Use the same risk window.
    TemporalPoint.ForPosition(8)); // Evaluate the audit at position 8.

var comparison = pipeline.Intervals // Start a detector comparison.
    .Compare("Risk detector audit") // Name the audit.
    .Target("risk-engine-a", selector => selector.Source("risk-engine-a")) // Treat engine A as target.
    .Against("risk-engine-b", selector => selector.Source("risk-engine-b")) // Compare engine B.
    .Within(scope => scope.Window("AccountRiskElevated")) // Scope to elevated-risk windows.
    .Normalize(normalization => normalization.KnownAtPosition(8)) // Reject windows unavailable at decision position 8.
    .Using(comparators => comparators.Overlap().Residual().AsOf(AsOfDirection.Previous, TemporalAxis.ProcessingPosition, toleranceMagnitude: 5)) // Audit agreement and prior corroboration.
    .Run(); // Execute the point-in-time-safe comparison.

Console.WriteLine("Security access audit"); // Print the sample title.
Console.WriteLine("annotation revision: " + annotation.Revision); // Show append-only metadata revisioning.
Console.WriteLine("known annotations at position 8: " + safeAnnotations.Count); // Show safe annotation count.
Console.WriteLine("overlap rows: " + comparison.OverlapRows.Count); // Show detector agreement.
Console.WriteLine("engine-a residual rows: " + comparison.ResidualRows.Count); // Show detector A-only evidence.
Console.WriteLine("as-of rows: " + comparison.AsOfRows.Count); // Show point-in-time lookup rows.

void Ingest(string source, int minute, int riskScore, string accessSurface) // Keep sample ingestion compact.
{
    pipeline.Ingest( // Send one risk update through Kyft.
        new AccountRiskSignal( // Create the risk event.
            "tenant-9", // Attach tenant context.
            "account-771", // Track one account.
            accessSurface, // Capture the current access surface.
            riskScore, // Carry the risk score.
            start.AddMinutes(minute)), // Use deterministic event time.
        source: source); // Store the detector as the source lane.
}

public sealed record AccountRiskSignal( // Define the event type.
    string TenantId, // Identifies the tenant.
    string AccountId, // Identifies the account.
    string AccessSurface, // Splits windows by access path.
    int RiskScore, // Drives the elevated-risk predicate.
    DateTimeOffset Timestamp); // Provides event-time bounds.
