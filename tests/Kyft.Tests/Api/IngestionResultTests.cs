using Kyft;

namespace Kyft.Tests.Api;

public sealed class IngestionResultTests
{
    [Fact]
    public void IngestReturnsEmptyResultWhenNoWindowsEmit()
    {
        var pipeline = Kyft.For<PriceTick>().Build();

        var result = pipeline.Ingest(new PriceTick("selection-1", 1.01m));

        Assert.Empty(result.Emissions);
        Assert.False(result.HasEmissions);
    }

    [Fact]
    public void IngestReturnsResultWithEmissions()
    {
        var pipeline = Kyft
            .For<PriceTick>()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m)
            .Build();

        var result = pipeline.Ingest(new PriceTick("selection-1", 0m));

        Assert.NotEmpty(result.Emissions);
        Assert.True(result.HasEmissions);
    }

    private sealed record PriceTick(string SelectionId, decimal Price);
}
