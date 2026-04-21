using Spanfold;

namespace Spanfold.Tests.Analysis;

public sealed class OverlapQueryTests
{
    [Fact]
    public void FindOverlapsReturnsOverlappingClosedWindowsInSameScope()
    {
        var history = new WindowHistory(enabled: true);
        var tick = new PriceTick("selection-1");

        history.Record(
        [
            new WindowEmission<PriceTick>(
                "SelectionSuspension",
                "selection-1",
                tick,
                WindowTransitionKind.Opened,
                Source: "provider-a")
        ], processingPosition: 1, eventTime: null);
        history.Record(
        [
            new WindowEmission<PriceTick>(
                "SelectionSuspension",
                "selection-1",
                tick,
                WindowTransitionKind.Opened,
                Source: "provider-b")
        ], processingPosition: 2, eventTime: null);
        history.Record(
        [
            new WindowEmission<PriceTick>(
                "SelectionSuspension",
                "selection-1",
                tick,
                WindowTransitionKind.Closed,
                Source: "provider-a")
        ], processingPosition: 4, eventTime: null);
        history.Record(
        [
            new WindowEmission<PriceTick>(
                "SelectionSuspension",
                "selection-1",
                tick,
                WindowTransitionKind.Closed,
                Source: "provider-b")
        ], processingPosition: 5, eventTime: null);

        var overlap = Assert.Single(history.FindOverlaps());
        Assert.Equal("provider-a", overlap.First.Source);
        Assert.Equal("provider-b", overlap.Second.Source);
    }

    [Fact]
    public void FindOverlapsIgnoresNonOverlappingWindows()
    {
        var history = new WindowHistory(enabled: true);
        var tick = new PriceTick("selection-1");

        RecordWindow(history, tick, source: "provider-a", start: 1, end: 2);
        RecordWindow(history, tick, source: "provider-b", start: 2, end: 3);

        Assert.Empty(history.FindOverlaps());
    }

    [Fact]
    public void FindOverlapsIgnoresDifferentScopes()
    {
        var history = new WindowHistory(enabled: true);
        var tick = new PriceTick("selection-1");

        RecordWindow(history, tick, source: "provider-a", key: "selection-1", start: 1, end: 4);
        RecordWindow(history, tick, source: "provider-b", key: "selection-2", start: 2, end: 5);

        Assert.Empty(history.FindOverlaps());
    }

    private static void RecordWindow(
        WindowHistory history,
        PriceTick tick,
        string source,
        long start,
        long end,
        string key = "selection-1")
    {
        history.Record(
        [
            new WindowEmission<PriceTick>(
                "SelectionSuspension",
                key,
                tick,
                WindowTransitionKind.Opened,
                Source: source)
        ], start, eventTime: null);
        history.Record(
        [
            new WindowEmission<PriceTick>(
                "SelectionSuspension",
                key,
                tick,
                WindowTransitionKind.Closed,
                Source: source)
        ], end, eventTime: null);
    }

    private sealed record PriceTick(string SelectionId);
}
