using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class SegmentCohortSafetyTests
{
    [Fact]
    public void ClosedPriorSegmentIsFinalWhileCurrentLiveSegmentIsProvisional()
    {
        var pipeline = Kyft
            .For<PriceUpdate>()
            .RecordIntervals()
            .TrackWindow("SelectionPriced", window => window
                .Key(update => update.SelectionId)
                .ActiveWhen(update => update.HasPrice)
                .Segment("phase", phase => phase.Value(update => update.Phase)));

        pipeline.Ingest(new PriceUpdate("selection-1", HasPrice: true, "Pregame"), source: "source-a");
        pipeline.Ingest(new PriceUpdate("selection-1", HasPrice: true, "InPlay"), source: "source-a");

        var pregame = pipeline.Intervals
            .Compare("Pregame finality")
            .Target("source-a", selector => selector.Source("source-a"))
            .Against("source-b", selector => selector.Source("source-b"))
            .Within(scope => scope.Window("SelectionPriced").Segment("phase", "Pregame"))
            .Using(comparators => comparators.Residual())
            .Run();
        var live = pipeline.Intervals
            .Compare("In-play finality")
            .Target("source-a", selector => selector.Source("source-a"))
            .Against("source-b", selector => selector.Source("source-b"))
            .Within(scope => scope.Window("SelectionPriced").Segment("phase", "InPlay"))
            .Using(comparators => comparators.Residual())
            .RunLive(TemporalPoint.ForPosition(3));

        Assert.All(pregame.RowFinalities, finality => Assert.Equal(ComparisonFinality.Final, finality.Finality));
        Assert.Contains(live.RowFinalities, finality => finality.Finality == ComparisonFinality.Provisional);
    }

    [Fact]
    public void KnownAtFilteringRunsBeforeCohortMaterialization()
    {
        var pipeline = CreatePipeline();

        AddClosedWindow(pipeline, source: "source-a", start: 1, end: 11);
        AddClosedWindow(pipeline, source: "source-b", start: 1, end: 6);
        AddClosedWindow(pipeline, source: "source-c", start: 6, end: 11);

        var knownAtFive = pipeline.Intervals
            .Compare("Known-at cohort")
            .Target("source-a", selector => selector.Source("source-a"))
            .AgainstCohort("cohort", cohort => cohort.Sources("source-b", "source-c"))
            .Within(scope => scope.Window("SelectionPriced"))
            .Normalize(normalization => normalization.KnownAtPosition(5))
            .Using(comparators => comparators.Residual())
            .Run();

        Assert.Empty(knownAtFive.Prepared!.NormalizedWindows);
        Assert.Contains(knownAtFive.Diagnostics, diagnostic =>
            diagnostic.Code == ComparisonPlanValidationCode.FutureWindowExcluded);
    }

    private static EventPipeline<PriceUpdate> CreatePipeline()
    {
        return Kyft
            .For<PriceUpdate>()
            .RecordIntervals()
            .TrackWindow("SelectionPriced", update => update.SelectionId, update => update.HasPrice);
    }

    private static void AddClosedWindow(
        EventPipeline<PriceUpdate> pipeline,
        string source,
        long start,
        long end)
    {
        var open = new WindowEmission<PriceUpdate>(
            "SelectionPriced",
            "selection-1",
            new PriceUpdate("selection-1", HasPrice: true, "InPlay"),
            WindowTransitionKind.Opened,
            source,
            Segments: [new WindowSegment("phase", "InPlay")]);
        var close = open with
        {
            Event = new PriceUpdate("selection-1", HasPrice: false, "InPlay"),
            Kind = WindowTransitionKind.Closed
        };

        pipeline.Intervals.Record([open], start, eventTime: null);
        pipeline.Intervals.Record([close], end, eventTime: null);
    }

    private sealed record PriceUpdate(string SelectionId, bool HasPrice, string Phase);
}
