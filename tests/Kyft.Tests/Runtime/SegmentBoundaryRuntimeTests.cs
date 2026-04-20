using Kyft;

namespace Kyft.Tests.Runtime;

public sealed class SegmentBoundaryRuntimeTests
{
    [Fact]
    public void SegmentValueChangeClosesAndReopensActiveWindow()
    {
        var pipeline = Kyft
            .For<PriceUpdate>()
            .RecordIntervals()
            .TrackWindow("SelectionPriced", window => window
                .Key(update => update.SelectionId)
                .ActiveWhen(update => update.HasPrice)
                .Segment("phase", phase => phase.Value(update => update.Phase)));

        pipeline.Ingest(new PriceUpdate("selection-1", HasPrice: true, "Pregame", Period: null));
        var transition = pipeline.Ingest(new PriceUpdate("selection-1", HasPrice: true, "InPlay", "FirstHalf"));

        Assert.Equal(
            [WindowTransitionKind.Closed, WindowTransitionKind.Opened],
            transition.Emissions.Select(emission => emission.Kind).ToArray());
        Assert.Equal(2, pipeline.Intervals.OpenWindows.Single().StartPosition);

        var closed = Assert.Single(pipeline.Intervals.ClosedWindows);
        Assert.Equal(1, closed.StartPosition);
        Assert.Equal(2, closed.EndPosition);
        Assert.Equal("Pregame", Assert.Single(closed.Segments).Value);

        var open = Assert.Single(pipeline.Intervals.OpenWindows);
        Assert.Equal("InPlay", Assert.Single(open.Segments).Value);
    }

    [Fact]
    public void NestedSegmentChangeSplitsCurrentWindow()
    {
        var pipeline = Kyft
            .For<PriceUpdate>()
            .RecordIntervals()
            .TrackWindow("SelectionPriced", window => window
                .Key(update => update.SelectionId)
                .ActiveWhen(update => update.HasPrice)
                .Segment("phase", PhaseWithPeriod));

        pipeline.Ingest(new PriceUpdate("selection-1", HasPrice: true, "InPlay", "FirstHalf"));
        pipeline.Ingest(new PriceUpdate("selection-1", HasPrice: true, "InPlay", "SecondHalf"));

        var closed = Assert.Single(pipeline.Intervals.ClosedWindows);
        Assert.Equal("period", closed.Segments[1].Name);
        Assert.Equal("FirstHalf", closed.Segments[1].Value);
        Assert.Equal("phase", closed.Segments[1].ParentName);

        var open = Assert.Single(pipeline.Intervals.OpenWindows);
        Assert.Equal("SecondHalf", open.Segments[1].Value);
    }

    [Fact]
    public void SameSegmentWhileActiveDoesNotEmitTransition()
    {
        var pipeline = Kyft
            .For<PriceUpdate>()
            .RecordIntervals()
            .TrackWindow("SelectionPriced", window => window
                .Key(update => update.SelectionId)
                .ActiveWhen(update => update.HasPrice)
                .Segment("phase", phase => phase.Value(update => update.Phase)));

        pipeline.Ingest(new PriceUpdate("selection-1", HasPrice: true, "InPlay", "FirstHalf"));
        var second = pipeline.Ingest(new PriceUpdate("selection-1", HasPrice: true, "InPlay", "FirstHalf"));

        Assert.False(second.HasEmissions);
        Assert.Empty(pipeline.Intervals.ClosedWindows);
        Assert.Single(pipeline.Intervals.OpenWindows);
    }

    [Fact]
    public void MissingSegmentValueSelectorIsRejected()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            Kyft
                .For<PriceUpdate>()
                .TrackWindow("SelectionPriced", window => window
                    .Key(update => update.SelectionId)
                    .ActiveWhen(update => update.HasPrice)
                    .Segment("phase", _ => { })));

        Assert.Contains("must configure a value selector", exception.Message);
    }

    [Fact]
    public void DuplicateSegmentNamesAreRejected()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            Kyft
                .For<PriceUpdate>()
                .TrackWindow("SelectionPriced", window => window
                    .Key(update => update.SelectionId)
                    .ActiveWhen(update => update.HasPrice)
                    .Segment("phase", phase => phase.Value(update => update.Phase))
                    .Segment("phase", phase => phase.Value(update => update.Period))));

        Assert.Contains("has already been configured", exception.Message);
    }

    private static void PhaseWithPeriod(SegmentBuilder<PriceUpdate> phase)
    {
        phase
            .Value(update => update.Phase)
            .Child("period", period => period.Value(update => update.Period));
    }

    private sealed record PriceUpdate(
        string SelectionId,
        bool HasPrice,
        string Phase,
        string? Period);
}
