using Kyft;

namespace Kyft.Tests.Intervals;

public sealed class ChunkTests
{
    [Fact]
    public void ClosedChunksStemFromChunk()
    {
        var interval = new ClosedChunk(
            "DeviceOffline",
            "device-1",
            StartPosition: 1,
            EndPosition: 2);

        var chunk = Assert.IsAssignableFrom<Chunk>(interval);
        Assert.True(chunk.IsClosed);
        Assert.Equal(2, chunk.EndPosition);
    }

    [Fact]
    public void OpenChunksStemFromChunk()
    {
        var interval = new OpenChunk(
            "DeviceOffline",
            "device-1",
            StartPosition: 1);

        var chunk = Assert.IsAssignableFrom<Chunk>(interval);
        Assert.False(chunk.IsClosed);
        Assert.Null(chunk.EndPosition);
    }

    [Fact]
    public void HistoryExposesOpenAndClosedChunksTogether()
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
            pipeline.Intervals.Chunks,
            closed => Assert.True(closed.IsClosed),
            open => Assert.False(open.IsClosed));
    }

    private sealed record DeviceSignal(string DeviceId, bool IsOnline);
}
