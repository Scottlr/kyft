using Kyft;

namespace Kyft.Tests.Recording;

public sealed class WindowRecordTests
{
    [Fact]
    public void ClosedWindowsStemFromWindowRecord()
    {
        var interval = new ClosedWindow(
            "DeviceOffline",
            "device-1",
            StartPosition: 1,
            EndPosition: 2);

        var window = Assert.IsAssignableFrom<WindowRecord>(interval);
        Assert.True(window.IsClosed);
        Assert.Equal(2, window.EndPosition);
    }

    [Fact]
    public void OpenWindowsStemFromWindowRecord()
    {
        var interval = new OpenWindow(
            "DeviceOffline",
            "device-1",
            StartPosition: 1);

        var window = Assert.IsAssignableFrom<WindowRecord>(interval);
        Assert.False(window.IsClosed);
        Assert.Null(window.EndPosition);
    }

    [Fact]
    public void HistoryExposesOpenAndClosedWindowsTogether()
    {
        var pipeline = Kyft
            .For<DeviceSignal>()
            .RecordIntervals()
            .TrackWindow(
                "DeviceOffline",
                signal => signal.DeviceId,
                signal => !signal.IsOnline);

        pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false));
        pipeline.Ingest(new DeviceSignal("device-1", IsOnline: true));
        pipeline.Ingest(new DeviceSignal("device-2", IsOnline: false));

        Assert.Collection(
            pipeline.Intervals.Windows,
            closed => Assert.True(closed.IsClosed),
            open => Assert.False(open.IsClosed));
    }

    private sealed record DeviceSignal(string DeviceId, bool IsOnline);
}
