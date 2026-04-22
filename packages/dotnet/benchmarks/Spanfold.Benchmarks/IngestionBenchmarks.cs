using BenchmarkDotNet.Attributes;
using Spanfold;

namespace Spanfold.Benchmarks;

[MemoryDiagnoser]
public class IngestionBenchmarks
{
    [Params(128, 1_024, 8_192)]
    public int EventCount { get; set; }

    [Benchmark]
    public WindowHistory IngestAndRecordWindows()
    {
        var pipeline = ComparisonBenchmarkData.CreatePipeline();

        for (var i = 0; i < EventCount; i++)
        {
            var deviceId = "device-" + (i % 256).ToString(System.Globalization.CultureInfo.InvariantCulture);
            var source = "provider-" + (i & 1).ToString(System.Globalization.CultureInfo.InvariantCulture);
            var isOnline = ((i / 2) & 1) == 0;
            pipeline.Ingest(new BenchmarkDeviceSignal(deviceId, isOnline), source);
        }

        return pipeline.History;
    }
}
