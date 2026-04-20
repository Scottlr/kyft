namespace Kyft.Benchmarks;

public sealed record SegmentCohortBenchmarkEvent(
    BenchmarkSegmentSignal Signal,
    string Source);
