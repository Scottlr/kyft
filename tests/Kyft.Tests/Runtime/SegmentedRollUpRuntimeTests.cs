using Kyft;

namespace Kyft.Tests.Runtime;

public sealed class SegmentedRollUpRuntimeTests
{
    [Fact]
    public void RollUpsPreserveChildSegmentContext()
    {
        var pipeline = Kyft
            .For<PriceUpdate>()
            .RecordIntervals()
            .Window("SelectionPriced", window => window
                .Key(update => update.SelectionId)
                .ActiveWhen(update => update.HasPrice)
                .Segment("phase", phase => phase.Value(update => update.Phase)))
            .RollUp("MarketPriced", update => update.MarketId, children => children.ActiveCount > 0)
            .Build();

        pipeline.Ingest(new PriceUpdate("selection-1", "market-1", "fixture-1", HasPrice: true, "Pregame"));
        pipeline.Ingest(new PriceUpdate("selection-1", "market-1", "fixture-1", HasPrice: true, "InPlay"));

        var closedMarket = Assert.Single(
            pipeline.Intervals.ClosedWindows,
            window => window.WindowName == "MarketPriced");
        Assert.Equal("Pregame", Assert.Single(closedMarket.Segments).Value);

        var openMarket = Assert.Single(
            pipeline.Intervals.OpenWindows,
            window => window.WindowName == "MarketPriced");
        Assert.Equal("InPlay", Assert.Single(openMarket.Segments).Value);
    }

    [Fact]
    public void MultiLevelRollUpsPreserveChildSegmentContext()
    {
        var pipeline = Kyft
            .For<PriceUpdate>()
            .RecordIntervals()
            .Window("SelectionPriced", window => window
                .Key(update => update.SelectionId)
                .ActiveWhen(update => update.HasPrice)
                .Segment("phase", phase => phase.Value(update => update.Phase)))
            .RollUp("MarketPriced", update => update.MarketId, children => children.ActiveCount > 0)
            .RollUp("FixturePriced", update => update.FixtureId, children => children.ActiveCount > 0)
            .Build();

        pipeline.Ingest(new PriceUpdate("selection-1", "market-1", "fixture-1", HasPrice: true, "Pregame"));
        pipeline.Ingest(new PriceUpdate("selection-1", "market-1", "fixture-1", HasPrice: true, "InPlay"));

        var closedFixture = Assert.Single(
            pipeline.Intervals.ClosedWindows,
            window => window.WindowName == "FixturePriced");
        Assert.Equal("Pregame", Assert.Single(closedFixture.Segments).Value);

        var openFixture = Assert.Single(
            pipeline.Intervals.OpenWindows,
            window => window.WindowName == "FixturePriced");
        Assert.Equal("InPlay", Assert.Single(openFixture.Segments).Value);
    }

    private sealed record PriceUpdate(
        string SelectionId,
        string MarketId,
        string FixtureId,
        bool HasPrice,
        string Phase);
}
