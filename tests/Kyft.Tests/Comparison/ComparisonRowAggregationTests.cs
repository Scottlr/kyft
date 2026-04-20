using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class ComparisonRowAggregationTests
{
    [Fact]
    public void ResidualRowsCanSumPositionLength()
    {
        var rows = new[]
        {
            CreateResidual(TemporalRange.Closed(TemporalPoint.ForPosition(1), TemporalPoint.ForPosition(4))),
            CreateResidual(TemporalRange.Closed(TemporalPoint.ForPosition(10), TemporalPoint.ForPosition(12)))
        };

        Assert.Equal(5, rows.TotalPositionLength());
    }

    [Fact]
    public void ResidualRowsCanSumTimeDuration()
    {
        var start = DateTimeOffset.Parse("2026-04-20T10:00:00Z");
        var rows = new[]
        {
            CreateResidual(TemporalRange.Closed(
                TemporalPoint.ForTimestamp(start),
                TemporalPoint.ForTimestamp(start.AddMinutes(5)))),
            CreateResidual(TemporalRange.Closed(
                TemporalPoint.ForTimestamp(start.AddMinutes(10)),
                TemporalPoint.ForTimestamp(start.AddMinutes(12))))
        };

        Assert.Equal(TimeSpan.FromMinutes(7), rows.TotalTimeDuration());
    }

    [Fact]
    public void MissingRowsCanSumPositionLength()
    {
        var rows = new[]
        {
            new MissingRow(
                "SelectionPriced",
                "selection-1",
                Partition: null,
                TemporalRange.Closed(TemporalPoint.ForPosition(1), TemporalPoint.ForPosition(3)),
                AgainstRecordIds: [])
        };

        Assert.Equal(2, rows.TotalPositionLength());
    }

    private static ResidualRow CreateResidual(TemporalRange range)
    {
        return new ResidualRow(
            "SelectionPriced",
            "selection-1",
            Partition: null,
            range,
            TargetRecordIds: []);
    }
}
