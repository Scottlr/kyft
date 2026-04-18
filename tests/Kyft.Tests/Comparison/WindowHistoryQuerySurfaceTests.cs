using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class WindowHistoryQuerySurfaceTests
{
    [Fact]
    public void CompareReturnsBuilderForHistory()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        var builder = history.Compare("Provider QA");

        Assert.Equal("Provider QA", builder.Name);
    }

    [Fact]
    public void CompareRejectsEmptyName()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        Assert.Throws<ArgumentException>(() => history.Compare(""));
    }

    [Fact]
    public void CompareDoesNotMutateHistory()
    {
        var pipeline = Kyft
            .For<DeviceSignal>()
            .RecordIntervals()
            .TrackWindow(
                "DeviceOffline",
                signal => signal.DeviceId,
                signal => !signal.IsOnline);

        pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false));

        _ = pipeline.Intervals.Compare("Provider QA");

        Assert.Single(pipeline.Intervals.OpenWindows);
        Assert.Empty(pipeline.Intervals.ClosedWindows);
    }

    [Fact]
    public void ExistingDirectQueriesRemainAvailable()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        Assert.Empty(history.FindOverlaps());
        Assert.Empty(history.FindResiduals("provider-a"));
    }

    private sealed record DeviceSignal(string DeviceId, bool IsOnline);
}
