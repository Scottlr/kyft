using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class CohortComparisonTests
{
    [Fact]
    public void ResidualAgainstAnyCohortDoesNotDoubleCountAlternatingMembers()
    {
        var pipeline = CreatePipeline();

        AddClosedWindow(pipeline, source: "source-a", start: 1, end: 11);
        AddClosedWindow(pipeline, source: "source-b", start: 1, end: 6);
        AddClosedWindow(pipeline, source: "source-c", start: 6, end: 11);

        var result = pipeline.Intervals
            .Compare("Source A vs cohort")
            .Target("source-a", selector => selector.Source("source-a"))
            .AgainstCohort("cohort", cohort => cohort
                .Sources("source-b", "source-c")
                .Activity(CohortActivity.Any()))
            .Within(scope => scope.Window("SelectionPriced"))
            .Using(comparators => comparators.Residual())
            .Run();

        Assert.Empty(result.ResidualRows);
    }

    [Fact]
    public void SegmentFiltersApplyToCohortResidual()
    {
        var pipeline = CreatePipeline();

        AddClosedWindow(pipeline, source: "source-a", start: 1, end: 11, tradingState: "Suspended");
        AddClosedWindow(pipeline, source: "source-b", start: 1, end: 6, tradingState: "Suspended");
        AddClosedWindow(pipeline, source: "source-c", start: 6, end: 11, tradingState: "Open");

        var result = pipeline.Intervals
            .Compare("Source A suspended vs cohort")
            .Target("source-a", selector => selector.Source("source-a"))
            .AgainstCohort("cohort", cohort => cohort.Sources("source-b", "source-c"))
            .Within(scope => scope
                .Window("SelectionPriced")
                .Segment("tradingState", "Suspended"))
            .Using(comparators => comparators.Residual())
            .Run();

        Assert.Equal(5, result.ResidualRows.TotalPositionLength());
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
        long end,
        string tradingState = "Suspended")
    {
        var open = new WindowEmission<PriceUpdate>(
            "SelectionPriced",
            "selection-1",
            new PriceUpdate("selection-1", HasPrice: true),
            WindowTransitionKind.Opened,
            source,
            Segments:
            [
                new WindowSegment("phase", "InPlay"),
                new WindowSegment("period", "FinalQuarter", ParentName: "phase"),
                new WindowSegment("tradingState", tradingState)
            ]);
        var close = open with
        {
            Event = new PriceUpdate("selection-1", HasPrice: false),
            Kind = WindowTransitionKind.Closed
        };

        pipeline.Intervals.Record([open], start, eventTime: null);
        pipeline.Intervals.Record([close], end, eventTime: null);
    }

    private sealed record PriceUpdate(string SelectionId, bool HasPrice);
}
