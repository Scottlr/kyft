using Spanfold;

namespace Spanfold.Tests.Temporal;

public sealed class TemporalPointTests
{
    [Fact]
    public void PositionPointsExposeAxisAndValue()
    {
        var point = TemporalPoint.ForPosition(12);

        Assert.Equal(TemporalAxis.ProcessingPosition, point.Axis);
        Assert.Equal(12, point.Position);
        Assert.Null(point.Clock);
    }

    [Fact]
    public void TimestampPointsExposeAxisValueAndClock()
    {
        var timestamp = new DateTimeOffset(2026, 4, 17, 10, 30, 0, TimeSpan.Zero);
        var point = TemporalPoint.ForTimestamp(timestamp, "event-time");

        Assert.Equal(TemporalAxis.Timestamp, point.Axis);
        Assert.Equal(timestamp, point.Timestamp);
        Assert.Equal("event-time", point.Clock);
    }

    [Fact]
    public void PositionPointsCompareByPosition()
    {
        var earlier = TemporalPoint.ForPosition(10);
        var later = TemporalPoint.ForPosition(20);

        Assert.True(earlier < later);
        Assert.True(later > earlier);
        Assert.True(earlier.IsBefore(later));
        Assert.True(later.IsAfter(earlier));
    }

    [Fact]
    public void TimestampPointsCompareByTimestampWhenClockMatches()
    {
        var earlier = TemporalPoint.ForTimestamp(
            new DateTimeOffset(2026, 4, 17, 10, 0, 0, TimeSpan.Zero),
            "event-time");
        var later = TemporalPoint.ForTimestamp(
            new DateTimeOffset(2026, 4, 17, 10, 1, 0, TimeSpan.Zero),
            "event-time");

        Assert.True(earlier < later);
        Assert.True(later > earlier);
    }

    [Fact]
    public void EqualPointsHaveValueEquality()
    {
        Assert.Equal(
            TemporalPoint.ForPosition(42),
            TemporalPoint.ForPosition(42));

        Assert.Equal(
            TemporalPoint.ForTimestamp(new DateTimeOffset(2026, 4, 17, 10, 0, 0, TimeSpan.Zero), "event-time"),
            TemporalPoint.ForTimestamp(new DateTimeOffset(2026, 4, 17, 10, 0, 0, TimeSpan.Zero), "event-time"));
    }

    [Fact]
    public void MixedAxesCannotBeCompared()
    {
        var position = TemporalPoint.ForPosition(10);
        var timestamp = TemporalPoint.ForTimestamp(
            new DateTimeOffset(2026, 4, 17, 10, 0, 0, TimeSpan.Zero));

        Assert.Throws<InvalidOperationException>(() => position.CompareTo(timestamp));
        Assert.Throws<InvalidOperationException>(() => position < timestamp);
    }

    [Fact]
    public void TimestampPointsWithDifferentClocksCannotBeCompared()
    {
        var providerTime = TemporalPoint.ForTimestamp(
            new DateTimeOffset(2026, 4, 17, 10, 0, 0, TimeSpan.Zero),
            "provider");
        var receivedTime = TemporalPoint.ForTimestamp(
            new DateTimeOffset(2026, 4, 17, 10, 0, 1, TimeSpan.Zero),
            "received");

        Assert.Throws<InvalidOperationException>(() => providerTime.CompareTo(receivedTime));
    }

    [Fact]
    public void WrongAxisValueAccessThrows()
    {
        var position = TemporalPoint.ForPosition(10);
        var timestamp = TemporalPoint.ForTimestamp(
            new DateTimeOffset(2026, 4, 17, 10, 0, 0, TimeSpan.Zero));

        Assert.Throws<InvalidOperationException>(() => position.Timestamp);
        Assert.Throws<InvalidOperationException>(() => timestamp.Position);
    }
}
