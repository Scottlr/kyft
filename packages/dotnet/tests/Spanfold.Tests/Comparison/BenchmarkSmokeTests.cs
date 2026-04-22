using Spanfold.Benchmarks;

namespace Spanfold.Tests.Comparison;

public sealed class BenchmarkSmokeTests
{
    [Fact]
    public void BenchmarkDataGenerationIsDeterministic()
    {
        var first = ComparisonBenchmarkData.Create(ComparisonScenario.Small);
        var second = ComparisonBenchmarkData.Create(ComparisonScenario.Small);

        Assert.Equal(first.EventCount, second.EventCount);
        Assert.Equal(first.DeviceCount, second.DeviceCount);
        Assert.Equal(first.SourceCount, second.SourceCount);
        Assert.Equal(first.History.ClosedWindows.Count, second.History.ClosedWindows.Count);
        Assert.Equal(first.History.OpenWindows.Count, second.History.OpenWindows.Count);
        Assert.Equal(first.History.Windows.Select(window => window.Id).ToArray(), second.History.Windows.Select(window => window.Id).ToArray());
    }

    [Fact]
    public void ComparisonBenchmarksCompileAndRunSmokePath()
    {
        var benchmarks = new ComparisonBenchmarks
        {
            Scenario = ComparisonScenario.Small
        };

        benchmarks.GlobalSetup();
        var prepared = benchmarks.Prepare();
        var aligned = benchmarks.Align();
        var result = benchmarks.RunMultiComparator();

        Assert.NotNull(benchmarks.GetDataForSmokeTest());
        Assert.NotEmpty(prepared.NormalizedWindows);
        Assert.NotEmpty(aligned.Segments);
        Assert.NotEmpty(result.ComparatorSummaries);
    }

    [Fact]
    public void SegmentCohortBenchmarksCompileAndRunSmokePath()
    {
        var benchmarks = new SegmentCohortBenchmarks
        {
            EventCount = 512
        };

        benchmarks.GlobalSetup();
        var history = benchmarks.IngestSegmentedRollUps();
        var segmentResult = benchmarks.RunSegmentFilteredResidual();
        var cohortResult = benchmarks.RunAnyCohortResidual();
        var liveResult = benchmarks.RunLiveSegmentCohortResidual();

        Assert.NotNull(benchmarks.GetDataForSmokeTest());
        Assert.NotEmpty(history.Windows);
        Assert.NotEmpty(segmentResult.ComparatorSummaries);
        Assert.NotEmpty(cohortResult.ComparatorSummaries);
        Assert.NotEmpty(liveResult.ComparatorSummaries);
    }
}
