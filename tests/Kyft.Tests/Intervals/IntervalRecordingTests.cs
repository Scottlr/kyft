using Kyft;

namespace Kyft.Tests.Intervals;

public sealed class IntervalRecordingTests
{
    [Fact]
    public void ClosedIntervalIsRecordedAfterOpenThenClose()
    {
        var pipeline = CreatePipeline();

        pipeline.Ingest(new PriceTick("selection-1", 0m), source: "provider-a", partition: "p1");
        pipeline.Ingest(new PriceTick("selection-1", 1.01m), source: "provider-a", partition: "p1");

        var interval = Assert.Single(pipeline.Intervals.ClosedChunks);
        Assert.Equal("SelectionSuspension", interval.WindowName);
        Assert.Equal("selection-1", interval.Key);
        Assert.Equal(1, interval.StartPosition);
        Assert.Equal(2, interval.EndPosition);
        Assert.Equal("provider-a", interval.Source);
        Assert.Equal("p1", interval.Partition);
    }

    [Fact]
    public void OpenWindowIsNotReportedAsClosedInterval()
    {
        var pipeline = CreatePipeline();

        pipeline.Ingest(new PriceTick("selection-1", 0m));

        Assert.Empty(pipeline.Intervals.ClosedChunks);

        var open = Assert.Single(pipeline.Intervals.OpenChunks);
        Assert.Equal("SelectionSuspension", open.WindowName);
        Assert.Equal(1, open.StartPosition);
    }

    [Fact]
    public void IntervalsAreNotRecordedUnlessEnabled()
    {
        var pipeline = Kyft
            .For<PriceTick>()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();

        pipeline.Ingest(new PriceTick("selection-1", 0m));
        pipeline.Ingest(new PriceTick("selection-1", 1.01m));

        Assert.Empty(pipeline.Intervals.ClosedChunks);
    }

    private static EventPipeline<PriceTick> CreatePipeline()
    {
        return Kyft
            .For<PriceTick>()
            .RecordIntervals()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();
    }

    private sealed record PriceTick(string SelectionId, decimal Price);
}
