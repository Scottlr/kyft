using Kyft;
using Kyft.Tests.Support;

namespace Kyft.Tests.Comparison;

public sealed class VirtualClockLiveTests
{
    [Fact]
    public void VirtualHorizonControlsOpenWindowClipping()
    {
        var clock = new VirtualComparisonClock();
        var pipeline = CreatePipeline();
        pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false), source: "provider-a");

        clock.AdvanceTo(10);
        var result = clock.Check(horizon => RunResidual(pipeline, horizon));

        var row = Assert.Single(result.ResidualRows);
        Assert.Equal(9, row.Range.GetPositionLength());
        Assert.Equal(ComparisonFinality.Provisional, Assert.Single(result.RowFinalities).Finality);
    }

    [Fact]
    public void LateEventInjectionRevisesExpectedRows()
    {
        var clock = new VirtualComparisonClock(10);
        var pipeline = CreatePipeline();
        pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false), source: "provider-a");

        var previous = clock.Check(horizon => RunResidual(pipeline, horizon));
        var current = clock.InjectLateEvent(
            () => pipeline.Ingest(new DeviceSignal("device-1", IsOnline: true), source: "provider-a"),
            horizon => RunResidual(pipeline, horizon));

        var entry = Assert.Single(ComparisonChangelog.Create(previous.RowFinalities, current.RowFinalities));
        Assert.Equal(ComparisonFinality.Revised, entry.Finality);
        Assert.Equal("residual[0]", entry.SupersedesRowId);
    }

    [Fact]
    public void LastWindowNotFlushedStillProducesProvisionalSnapshot()
    {
        var clock = new VirtualComparisonClock(25);
        var pipeline = CreatePipeline();
        pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false), source: "provider-a");

        var result = clock.Check(horizon => RunResidual(pipeline, horizon));

        Assert.Single(pipeline.History.OpenWindows);
        Assert.Empty(pipeline.History.ClosedWindows);
        Assert.Single(result.ResidualRows);
        Assert.Equal(ComparisonFinality.Provisional, Assert.Single(result.RowFinalities).Finality);
    }

    [Fact]
    public void VirtualClockRejectsBackwardsTime()
    {
        var clock = new VirtualComparisonClock(10);

        Assert.Throws<ArgumentOutOfRangeException>(() => clock.AdvanceTo(9));
    }

    private static ComparisonResult RunResidual(EventPipeline<DeviceSignal> pipeline, TemporalPoint horizon)
    {
        return pipeline.History.Compare("Virtual live QA")
            .Target("provider-a", selector => selector.Source("provider-a"))
            .Against("provider-b", selector => selector.Source("provider-b"))
            .Within(scope => scope.Window("DeviceOffline"))
            .Using(comparators => comparators.Residual())
            .RunLive(horizon);
    }

    private static EventPipeline<DeviceSignal> CreatePipeline()
    {
        return Kyft
            .For<DeviceSignal>()
            .RecordWindows()
            .TrackWindow("DeviceOffline", signal => signal.DeviceId, signal => !signal.IsOnline);
    }

    private sealed record DeviceSignal(string DeviceId, bool IsOnline);
}
