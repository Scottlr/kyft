using Kyft;

namespace Kyft.Tests.Intervals;

public sealed class EventTimeIntervalTests
{
    [Fact]
    public void IntervalTimestampsComeFromOpeningAndClosingEvents()
    {
        var openedAt = new DateTimeOffset(2026, 4, 12, 10, 0, 0, TimeSpan.Zero);
        var closedAt = new DateTimeOffset(2026, 4, 12, 10, 5, 0, TimeSpan.Zero);
        var pipeline = Kyft
            .For<PriceTick>()
            .RecordIntervals()
            .WithEventTime(tick => tick.Timestamp)
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();

        pipeline.Ingest(new PriceTick("selection-1", 0m, openedAt));
        pipeline.Ingest(new PriceTick("selection-1", 1.01m, closedAt));

        var interval = Assert.Single(pipeline.Intervals.ClosedChunks);
        Assert.Equal(openedAt, interval.StartTime);
        Assert.Equal(closedAt, interval.EndTime);
        Assert.Equal(1, interval.StartPosition);
        Assert.Equal(2, interval.EndPosition);
    }

    [Fact]
    public void IntervalTimestampsAreEmptyWhenEventTimeIsNotConfigured()
    {
        var pipeline = Kyft
            .For<PriceTick>()
            .RecordIntervals()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();

        pipeline.Ingest(new PriceTick("selection-1", 0m, DateTimeOffset.UtcNow));
        pipeline.Ingest(new PriceTick("selection-1", 1.01m, DateTimeOffset.UtcNow));

        var interval = Assert.Single(pipeline.Intervals.ClosedChunks);
        Assert.Null(interval.StartTime);
        Assert.Null(interval.EndTime);
    }

    [Fact]
    public void EventTimeSelectorIsRequired()
    {
        var builder = Kyft.For<PriceTick>();

        Assert.Throws<ArgumentNullException>(() => builder.WithEventTime(null!));
    }

    private sealed record PriceTick(
        string SelectionId,
        decimal Price,
        DateTimeOffset Timestamp);
}
