using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class FluentComparisonBuilderTests
{
    [Fact]
    public void ProviderQaExampleBuildsPlan()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        var plan = history.Compare("Provider QA")
            .Target("provider-a", s => s.Source("provider-a"))
            .Against("provider-b", s => s.Source("provider-b"))
            .Within(s => s.Window("DeviceOffline"))
            .Normalize(n => n.RequireClosedWindows().HalfOpen())
            .Using(c => c.Overlap().Residual().Coverage())
            .Build();

        Assert.Empty(plan.Validate());
        Assert.Equal("provider-a", plan.Target?.Name);
        Assert.Equal("provider-b", Assert.Single(plan.Against).Name);
        Assert.Equal(["overlap", "residual", "coverage"], plan.Comparators);
    }

    [Fact]
    public void MissingStagesProduceDiagnostics()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        var diagnostics = history.Compare("Provider QA").Validate();

        Assert.Contains(diagnostics, d => d.Code == ComparisonPlanValidationCode.MissingTarget);
        Assert.Contains(diagnostics, d => d.Code == ComparisonPlanValidationCode.MissingAgainst);
        Assert.Contains(diagnostics, d => d.Code == ComparisonPlanValidationCode.MissingScope);
        Assert.Contains(diagnostics, d => d.Code == ComparisonPlanValidationCode.MissingComparator);
    }

    [Fact]
    public void MultipleAgainstSelectorsAreSupported()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        var plan = history.Compare("Provider QA")
            .Target("provider-a", s => s.Source("provider-a"))
            .Against("provider-b", s => s.Source("provider-b"))
            .Against("provider-c", s => s.Source("provider-c"))
            .Within(s => s.Window("DeviceOffline"))
            .Using(c => c.Overlap())
            .Build();

        Assert.Equal(["provider-b", "provider-c"], plan.Against.Select(a => a.Name).ToArray());
    }

    [Fact]
    public void BuildDoesNotExecuteOrMutateHistory()
    {
        var pipeline = Kyft
            .For<DeviceSignal>()
            .RecordIntervals()
            .TrackWindow("DeviceOffline", signal => signal.DeviceId, signal => !signal.IsOnline);

        pipeline.Ingest(new DeviceSignal("device-1", IsOnline: false));

        _ = pipeline.Intervals.Compare("Provider QA")
            .Target("provider-a", s => s.Source("provider-a"))
            .Against("provider-b", s => s.Source("provider-b"))
            .Within(s => s.Window("DeviceOffline"))
            .Using(c => c.Overlap())
            .Build();

        Assert.Single(pipeline.Intervals.OpenWindows);
        Assert.Empty(pipeline.Intervals.ClosedWindows);
    }

    [Fact]
    public void PrepareAndRunReturnValidationArtifacts()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        var builder = history.Compare("Provider QA")
            .Target("provider-a", s => s.Source("provider-a"))
            .Against("provider-b", s => s.Source("provider-b"))
            .Within(s => s.Window("DeviceOffline"))
            .Using(c => c.Overlap());

        var prepared = builder.Prepare();
        var result = builder.Run();

        Assert.Empty(prepared.Diagnostics);
        Assert.True(result.IsValid);
        Assert.Equal("Provider QA", result.Plan.Name);
    }

    private sealed record DeviceSignal(string DeviceId, bool IsOnline);
}
