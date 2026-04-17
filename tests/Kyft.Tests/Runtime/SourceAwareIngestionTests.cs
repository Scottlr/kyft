using Kyft;

namespace Kyft.Tests.Runtime;

public sealed class SourceAwareIngestionTests
{
    [Fact]
    public void IngestCanAttachSourceToEmissions()
    {
        var pipeline = Kyft
            .For<PriceTick>()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();

        var result = pipeline.Ingest(new PriceTick("selection-1", 0m), source: "provider-a");

        var emission = Assert.Single(result.Emissions);
        Assert.Equal("provider-a", emission.Source);
    }

    [Fact]
    public void IngestWithoutSourceLeavesEmissionSourceEmpty()
    {
        var pipeline = Kyft
            .For<PriceTick>()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();

        var result = pipeline.Ingest(new PriceTick("selection-1", 0m));

        var emission = Assert.Single(result.Emissions);
        Assert.Null(emission.Source);
    }

    private sealed record PriceTick(string SelectionId, decimal Price);
}
