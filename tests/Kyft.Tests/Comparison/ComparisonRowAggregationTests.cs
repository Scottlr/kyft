using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class ComparisonRowAggregationTests
{
    [Fact]
    public void OverlapRowsCanSumPositionLength()
    {
        var rows = new[]
        {
            new OverlapRow(
                "SelectionPriced",
                "selection-1",
                Partition: null,
                TemporalRange.Closed(TemporalPoint.ForPosition(1), TemporalPoint.ForPosition(4)),
                TargetRecordIds: [],
                AgainstRecordIds: []),
            new OverlapRow(
                "SelectionPriced",
                "selection-1",
                Partition: null,
                TemporalRange.Closed(TemporalPoint.ForPosition(6), TemporalPoint.ForPosition(8)),
                TargetRecordIds: [],
                AgainstRecordIds: [])
        };

        Assert.Equal(5, rows.TotalPositionLength());
    }

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

    [Fact]
    public void CoverageRowsCanSumMagnitudes()
    {
        var rows = new[]
        {
            new CoverageRow(
                "SelectionPriced",
                "selection-1",
                Partition: null,
                TemporalRange.Closed(TemporalPoint.ForPosition(1), TemporalPoint.ForPosition(4)),
                TargetMagnitude: 3,
                CoveredMagnitude: 2,
                TargetRecordIds: [],
                AgainstRecordIds: []),
            new CoverageRow(
                "SelectionPriced",
                "selection-1",
                Partition: null,
                TemporalRange.Closed(TemporalPoint.ForPosition(4), TemporalPoint.ForPosition(6)),
                TargetMagnitude: 2,
                CoveredMagnitude: 1,
                TargetRecordIds: [],
                AgainstRecordIds: [])
        };

        Assert.Equal(5, rows.TotalPositionLength());
        Assert.Equal(5d, rows.TotalTargetMagnitude());
        Assert.Equal(3d, rows.TotalCoveredMagnitude());
    }

    [Fact]
    public void GapRowsCanSumPositionLength()
    {
        var rows = new[]
        {
            new GapRow(
                "SelectionPriced",
                "selection-1",
                Partition: null,
                TemporalRange.Closed(TemporalPoint.ForPosition(1), TemporalPoint.ForPosition(4)))
        };

        Assert.Equal(3, rows.TotalPositionLength());
    }

    [Fact]
    public void SymmetricDifferenceRowsCanSumTimeDuration()
    {
        var start = DateTimeOffset.Parse("2026-04-20T10:00:00Z");
        var rows = new[]
        {
            new SymmetricDifferenceRow(
                "SelectionPriced",
                "selection-1",
                Partition: null,
                TemporalRange.Closed(
                    TemporalPoint.ForTimestamp(start),
                    TemporalPoint.ForTimestamp(start.AddMinutes(4))),
                ComparisonSide.Target,
                TargetRecordIds: [],
                AgainstRecordIds: [])
        };

        Assert.Equal(TimeSpan.FromMinutes(4), rows.TotalTimeDuration());
    }

    [Fact]
    public void EmptyRowCollectionsReturnZero()
    {
        Assert.Equal(0, Array.Empty<ResidualRow>().TotalPositionLength());
        Assert.Equal(TimeSpan.Zero, Array.Empty<MissingRow>().TotalTimeDuration());
        Assert.Equal(0d, Array.Empty<CoverageRow>().TotalCoveredMagnitude());
    }

    [Fact]
    public void WrongAxisAggregationThrows()
    {
        var rows = new[]
        {
            CreateResidual(TemporalRange.Closed(TemporalPoint.ForPosition(1), TemporalPoint.ForPosition(4)))
        };

        Assert.Throws<InvalidOperationException>(() => rows.TotalTimeDuration());
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
