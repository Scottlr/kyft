using Spanfold;

namespace Spanfold.Tests.Api;

public sealed class WindowDefinitionApiTests
{
    [Fact]
    public void WindowDefinesStateDrivenWindow()
    {
        var builder = Spanfold
            .For<PriceTick>()
            .Window(
                "SelectionSuspension",
                key: tick => tick.SelectionId,
                isActive: tick => tick.Price == 0m);

        Assert.IsType<WindowPipelineBuilder<PriceTick>>(builder);
    }

    [Fact]
    public void WindowRequiresName()
    {
        var builder = Spanfold.For<PriceTick>();

        Assert.Throws<ArgumentException>(() => builder.Window(
            "",
            key: tick => tick.SelectionId,
            isActive: tick => tick.Price == 0m));
    }

    [Fact]
    public void WindowRequiresKeySelector()
    {
        var builder = Spanfold.For<PriceTick>();

        Assert.Throws<ArgumentNullException>(() => builder.Window<string>(
            "SelectionSuspension",
            key: null!,
            isActive: tick => tick.Price == 0m));
    }

    [Fact]
    public void WindowRequiresActiveSelector()
    {
        var builder = Spanfold.For<PriceTick>();

        Assert.Throws<ArgumentNullException>(() => builder.Window(
            "SelectionSuspension",
            key: tick => tick.SelectionId,
            isActive: null!));
    }

    private sealed record PriceTick(string SelectionId, decimal Price);
}
