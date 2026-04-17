using Kyft;

namespace Kyft.Tests.Api;

public sealed class ReadmeExampleTests
{
    [Fact]
    public void ReadmeDeviceToZoneExampleCompilesAndRuns()
    {
        var pipeline = Kyft
            .For<DeviceSignal>()
            .RecordIntervals()
            .Window(
                "DeviceOffline",
                key: signal => signal.DeviceId,
                isActive: signal => !signal.IsOnline)
            .RollUp(
                "ZoneOutage",
                key: signal => signal.ZoneId,
                isActive: children => children.AllActive())
            .Build();

        var opened = pipeline.Ingest(new DeviceSignal(
            DeviceId: "device-1",
            ZoneId: "zone-a",
            IsOnline: false));

        Assert.NotEmpty(opened.Emissions);
    }

    private sealed record DeviceSignal(
        string DeviceId,
        string ZoneId,
        bool IsOnline);
}
