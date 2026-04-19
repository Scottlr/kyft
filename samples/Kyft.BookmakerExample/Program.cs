using Kyft;

var start = DateTimeOffset.Parse("2026-04-19T12:00:00Z");
var pipeline = Kyft.Kyft
    .For<OddsAvailability>()
    .RecordIntervals()
    .WithEventTime(tick => tick.Timestamp)
    .Window(
        "SelectionUnavailable",
        key: tick => tick.SelectionId,
        isActive: tick => !tick.IsOffered)
    .RollUp(
        "MarketSuspended",
        key: tick => tick.MarketId,
        isActive: children => children.AnyActive())
    .Build();

Ingest("provider-a", 0, offered: true);
Ingest("provider-b", 1, offered: true);
Ingest("provider-a", 2, offered: false);
Ingest("provider-b", 4, offered: false);
Ingest("provider-b", 8, offered: true);
Ingest("provider-a", 10, offered: true);

var historical = pipeline.Intervals
    .Compare("Provider suspension QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("SelectionUnavailable"))
    .Using(comparators => comparators
        .Overlap()
        .Residual()
        .Coverage()
        .LeadLag(LeadLagTransition.Start, TemporalAxis.ProcessingPosition, toleranceMagnitude: 2))
    .Run();

Console.WriteLine("Historical provider QA");
Console.WriteLine("overlap rows: " + historical.OverlapRows.Count);
Console.WriteLine("provider-a residual rows: " + historical.ResidualRows.Count);
Console.WriteLine("coverage summaries: " + historical.CoverageSummaries.Count);
Console.WriteLine("lead/lag rows: " + historical.LeadLagRows.Count);

Ingest("provider-a", 14, offered: false);

var live = pipeline.Intervals
    .Compare("Live suspension QA")
    .Target("provider-a", selector => selector.Source("provider-a"))
    .Against("provider-b", selector => selector.Source("provider-b"))
    .Within(scope => scope.Window("SelectionUnavailable"))
    .Using(comparators => comparators.Residual())
    .RunLive(TemporalPoint.ForPosition(20));

Console.WriteLine();
Console.WriteLine("Live provider QA");
Console.WriteLine("residual rows: " + live.ResidualRows.Count);
Console.WriteLine("provisional rows: " + live.RowFinalities.Count(finality => finality.Finality == ComparisonFinality.Provisional));
Console.WriteLine();
Console.WriteLine(live.ExportMarkdown());

void Ingest(string provider, int minuteOffset, bool offered)
{
    pipeline.Ingest(
        new OddsAvailability(
            FixtureId: "fixture-101",
            MarketId: "match-winner",
            SelectionId: "home-win",
            Provider: provider,
            IsOffered: offered,
            Timestamp: start.AddMinutes(minuteOffset)),
        source: provider);
}

public sealed record OddsAvailability(
    string FixtureId,
    string MarketId,
    string SelectionId,
    string Provider,
    bool IsOffered,
    DateTimeOffset Timestamp);
