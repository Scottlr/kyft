namespace Spanfold.Benchmarks;

public sealed record SegmentCohortBenchmarkEvent(
    BenchmarkSegmentSignal Signal,
    string Source);
