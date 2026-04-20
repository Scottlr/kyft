namespace Kyft.Benchmarks;

public sealed record BenchmarkSegmentSignal(
    string DeviceId,
    string MarketId,
    string FixtureId,
    bool IsOnline,
    string Phase,
    string Period,
    string State);
