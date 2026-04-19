using BenchmarkDotNet.Attributes;
using Kyft;

namespace Kyft.Benchmarks;

[MemoryDiagnoser]
public class ComparisonBenchmarks
{
    private ComparisonBenchmarkData data = null!;

    [Params(
        ComparisonScenario.Small,
        ComparisonScenario.Medium,
        ComparisonScenario.HighOverlap,
        ComparisonScenario.HighCardinality,
        ComparisonScenario.ManySource)]
    public ComparisonScenario Scenario { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        this.data = ComparisonBenchmarkData.Create(Scenario);
    }

    [Benchmark]
    public PreparedComparison Prepare()
    {
        return CreateBaseBuilder()
            .Using(comparators => comparators.Overlap())
            .Prepare();
    }

    [Benchmark]
    public AlignedComparison Align()
    {
        return CreateBaseBuilder()
            .Using(comparators => comparators.Overlap())
            .Prepare()
            .Align();
    }

    [Benchmark]
    public ComparisonResult RunOverlap()
    {
        return CreateBaseBuilder()
            .Using(comparators => comparators.Overlap())
            .Run();
    }

    [Benchmark]
    public ComparisonResult RunResidual()
    {
        return CreateBaseBuilder()
            .Using(comparators => comparators.Residual())
            .Run();
    }

    [Benchmark]
    public ComparisonResult RunCoverage()
    {
        return CreateBaseBuilder()
            .Using(comparators => comparators.Coverage())
            .Run();
    }

    [Benchmark]
    public ComparisonResult RunMultiComparator()
    {
        return CreateBaseBuilder()
            .Using(comparators => comparators.Overlap().Residual().Missing().Coverage().Gap().SymmetricDifference())
            .Run();
    }

    public ComparisonBenchmarkData GetDataForSmokeTest()
    {
        return this.data;
    }

    private WindowComparisonBuilder CreateBaseBuilder()
    {
        return this.data.History.Compare("Benchmark Provider QA")
            .Target("provider-0", selector => selector.Source("provider-0"))
            .Against("provider-1", selector => selector.Source("provider-1"))
            .Within(scope => scope.Window("DeviceOffline"));
    }
}
