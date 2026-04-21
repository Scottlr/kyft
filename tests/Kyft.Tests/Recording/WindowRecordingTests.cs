using Kyft;

namespace Kyft.Tests.Recording;

public sealed class WindowRecordingTests
{
    [Fact]
    public void ClosedWindowIsRecordedAfterOpenThenClose()
    {
        var pipeline = CreatePipeline();

        pipeline.Ingest(new PriceTick("selection-1", 0m), source: "provider-a", partition: "p1");
        pipeline.Ingest(new PriceTick("selection-1", 1.01m), source: "provider-a", partition: "p1");

        var window = Assert.Single(pipeline.History.ClosedWindows);
        Assert.Equal("SelectionSuspension", window.WindowName);
        Assert.Equal("selection-1", window.Key);
        Assert.Equal(1, window.StartPosition);
        Assert.Equal(2, window.EndPosition);
        Assert.Equal("provider-a", window.Source);
        Assert.Equal("p1", window.Partition);
    }

    [Fact]
    public void OpenWindowIsNotReportedAsClosedWindow()
    {
        var pipeline = CreatePipeline();

        pipeline.Ingest(new PriceTick("selection-1", 0m));

        Assert.Empty(pipeline.History.ClosedWindows);

        var open = Assert.Single(pipeline.History.OpenWindows);
        Assert.Equal("SelectionSuspension", open.WindowName);
        Assert.Equal(1, open.StartPosition);
    }

    [Fact]
    public void WindowsAreNotRecordedUnlessEnabled()
    {
        var pipeline = Kyft
            .For<PriceTick>()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();

        pipeline.Ingest(new PriceTick("selection-1", 0m));
        pipeline.Ingest(new PriceTick("selection-1", 1.01m));

        Assert.Empty(pipeline.History.ClosedWindows);
    }

    private static EventPipeline<PriceTick> CreatePipeline()
    {
        return Kyft
            .For<PriceTick>()
            .RecordWindows()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();
    }

    private sealed record PriceTick(string SelectionId, decimal Price);
}
