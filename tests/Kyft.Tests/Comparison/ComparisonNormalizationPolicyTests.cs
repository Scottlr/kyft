using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class ComparisonNormalizationPolicyTests
{
    [Fact]
    public void DefaultPolicyUsesClosedHalfOpenProcessingPositionRanges()
    {
        var policy = ComparisonNormalizationPolicy.Default;

        Assert.True(policy.RequireClosedWindows);
        Assert.True(policy.UseHalfOpenRanges);
        Assert.Equal(TemporalAxis.ProcessingPosition, policy.TimeAxis);
        Assert.Equal(ComparisonOpenWindowPolicy.RequireClosed, policy.OpenWindowPolicy);
        Assert.Null(policy.OpenWindowHorizon);
        Assert.Equal(ComparisonNullTimestampPolicy.Reject, policy.NullTimestampPolicy);
    }

    [Fact]
    public void BuilderCanSelectEventTimeAndMissingTimestampPolicy()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        var plan = history.Compare("Event Time QA")
            .Target("provider-a", s => s.Source("provider-a"))
            .Against("provider-b", s => s.Source("provider-b"))
            .Within(s => s.Window("DeviceOffline"))
            .Normalize(n => n.OnEventTime().ExcludeMissingEventTime())
            .Using(c => c.Overlap())
            .Build();

        Assert.Equal(TemporalAxis.Timestamp, plan.Normalization.TimeAxis);
        Assert.Equal(ComparisonNullTimestampPolicy.Exclude, plan.Normalization.NullTimestampPolicy);
    }

    [Fact]
    public void BuilderCanClipOpenWindowsToHorizon()
    {
        var horizon = TemporalPoint.ForPosition(100);
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        var plan = history.Compare("Live QA")
            .Target("provider-a", s => s.Source("provider-a"))
            .Against("provider-b", s => s.Source("provider-b"))
            .Within(s => s.Window("DeviceOffline"))
            .Normalize(n => n.ClipOpenWindowsTo(horizon))
            .Using(c => c.Overlap())
            .Build();

        Assert.False(plan.Normalization.RequireClosedWindows);
        Assert.Equal(ComparisonOpenWindowPolicy.ClipToHorizon, plan.Normalization.OpenWindowPolicy);
        Assert.Equal(horizon, plan.Normalization.OpenWindowHorizon);
    }

    [Fact]
    public void BuilderCanSetCoalescingAndDuplicatePolicy()
    {
        var history = Kyft.For<DeviceSignal>().RecordIntervals().Build().Intervals;

        var plan = history.Compare("Provider QA")
            .Target("provider-a", s => s.Source("provider-a"))
            .Against("provider-b", s => s.Source("provider-b"))
            .Within(s => s.Window("DeviceOffline"))
            .Normalize(n => n.CoalesceAdjacentWindows().RejectDuplicateWindows())
            .Using(c => c.Overlap())
            .Build();

        Assert.True(plan.Normalization.CoalesceAdjacentWindows);
        Assert.Equal(ComparisonDuplicateWindowPolicy.Reject, plan.Normalization.DuplicateWindowPolicy);
    }

    private sealed record DeviceSignal(string DeviceId, bool IsOnline);
}
