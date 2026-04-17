using Kyft;

namespace Kyft.Tests.Analysis;

public sealed class ResidualAnalysisTests
{
    [Fact]
    public void FullOverlapLeavesNoResidualSegments()
    {
        var history = new WindowIntervalHistory(enabled: true);
        var tick = new PriceTick("selection-1");

        RecordInterval(history, tick, source: "provider-a", start: 1, end: 5);
        RecordInterval(history, tick, source: "provider-b", start: 1, end: 5);

        Assert.Empty(history.FindResiduals("provider-a"));
    }

    [Fact]
    public void PartialOverlapLeavesUniqueTargetSegment()
    {
        var history = new WindowIntervalHistory(enabled: true);
        var tick = new PriceTick("selection-1");

        RecordInterval(history, tick, source: "provider-a", start: 1, end: 5);
        RecordInterval(history, tick, source: "provider-b", start: 3, end: 6);

        var residual = Assert.Single(history.FindResiduals("provider-a"));
        Assert.Equal("provider-a", residual.Source);
        Assert.Equal(1, residual.StartPosition);
        Assert.Equal(3, residual.EndPosition);
    }

    [Fact]
    public void NoOverlapLeavesFullTargetSegment()
    {
        var history = new WindowIntervalHistory(enabled: true);
        var tick = new PriceTick("selection-1");

        RecordInterval(history, tick, source: "provider-a", start: 1, end: 3);
        RecordInterval(history, tick, source: "provider-b", start: 3, end: 5);

        var residual = Assert.Single(history.FindResiduals("provider-a"));
        Assert.Equal(1, residual.StartPosition);
        Assert.Equal(3, residual.EndPosition);
    }

    [Fact]
    public void TargetSourceIsRequired()
    {
        var history = new WindowIntervalHistory(enabled: true);

        Assert.Throws<ArgumentNullException>(() => history.FindResiduals(null!));
    }

    private static void RecordInterval(
        WindowIntervalHistory history,
        PriceTick tick,
        string source,
        long start,
        long end)
    {
        history.Record(
        [
            new WindowEmission<PriceTick>(
                "SelectionSuspension",
                tick.SelectionId,
                tick,
                WindowTransitionKind.Opened,
                Source: source)
        ], start, eventTime: null);
        history.Record(
        [
            new WindowEmission<PriceTick>(
                "SelectionSuspension",
                tick.SelectionId,
                tick,
                WindowTransitionKind.Closed,
                Source: source)
        ], end, eventTime: null);
    }

    private sealed record PriceTick(string SelectionId);
}
