using Kyft;

var start = DateTimeOffset.Parse("2026-04-21T11:00:00Z");

var pipeline = Kyft.Kyft
    .For<AccountRiskSignal>()
    .RecordWindows()
    .WithEventTime(signal => signal.Timestamp)
    .Window("AccountRiskElevated", window => window
        .Key(signal => signal.AccountId)
        .ActiveWhen(signal => signal.RiskScore >= 70)
        .Segment("accessSurface", segment => segment.Value(signal => signal.AccessSurface))
        .Tag("tenant", signal => signal.TenantId))
    .Build();

PrintScenario();

// The audit uses known-at filtering so the comparison cannot see future
// windows or annotations that were not available at the decision point.
Ingest("risk-engine-a", 0, 10, "console");
Ingest("risk-engine-b", 0, 12, "console");
Ingest("risk-engine-a", 4, 85, "console");
Ingest("risk-engine-b", 6, 78, "console");
Ingest("risk-engine-a", 12, 40, "console");
Ingest("risk-engine-b", 16, 35, "console");

var elevated = pipeline.History.Query()
    .Window("AccountRiskElevated")
    .Lane("risk-engine-a")
    .LatestWindow();

var annotation = pipeline.History.Annotate(
    elevated!,
    "rootCause",
    "impossible-travel",
    TemporalPoint.ForPosition(7));

var safeAnnotations = pipeline.History.AnnotationsKnownAt(elevated!, TemporalPoint.ForPosition(8));

var comparison = pipeline.History
    .Compare("Risk detector audit")
    .Target("risk-engine-a", selector => selector.Source("risk-engine-a"))
    .Against("risk-engine-b", selector => selector.Source("risk-engine-b"))
    .Within(scope => scope.Window("AccountRiskElevated"))
    .Normalize(normalization => normalization.KnownAtPosition(8))
    .Using(comparators => comparators.Overlap().Residual().AsOf(AsOfDirection.Previous, TemporalAxis.ProcessingPosition, toleranceMagnitude: 5))
    .Run();

Console.WriteLine("Security access audit");
Console.WriteLine("annotation revision: " + annotation.Revision);
Console.WriteLine("known annotations at position 8: " + safeAnnotations.Count);
Console.WriteLine("overlap rows: " + comparison.OverlapRows.Count);
Console.WriteLine("engine-a residual rows: " + comparison.ResidualRows.Count);
Console.WriteLine("as-of rows: " + comparison.AsOfRows.Count);

void PrintScenario()
{
    Console.WriteLine(
        """
        Scenario
        --------
        decision horizon: position 8

        engine-a risk: elevated 04..12
        engine-b risk: elevated 06..16
        annotation:    rootCause known at position 7

        The audit can use the annotation at position 8, but cannot leak later data.

        """);
}

void Ingest(string source, int minute, int riskScore, string accessSurface)
{
    pipeline.Ingest(
        new AccountRiskSignal("tenant-9", "account-771", accessSurface, riskScore, start.AddMinutes(minute)),
        source: source);
}

public sealed record AccountRiskSignal(
    string TenantId,
    string AccountId,
    string AccessSurface,
    int RiskScore,
    DateTimeOffset Timestamp);
