using Kyft;

namespace Kyft.Tests.Api;

public sealed class WindowEmissionModelTests
{
    [Fact]
    public void EmissionRepresentsOpenedWindow()
    {
        var tick = new PriceTick("selection-1", 0m);
        var emission = new WindowEmission<PriceTick>(
            "SelectionSuspension",
            tick.SelectionId,
            tick,
            WindowTransitionKind.Opened);

        Assert.Equal("SelectionSuspension", emission.WindowName);
        Assert.Equal("selection-1", emission.Key);
        Assert.Same(tick, emission.Event);
        Assert.Equal(WindowTransitionKind.Opened, emission.Kind);
    }

    [Fact]
    public void EmissionRepresentsClosedWindow()
    {
        var tick = new PriceTick("selection-1", 1.01m);
        var emission = new WindowEmission<PriceTick>(
            "SelectionSuspension",
            tick.SelectionId,
            tick,
            WindowTransitionKind.Closed);

        Assert.Equal(WindowTransitionKind.Closed, emission.Kind);
    }

    private sealed record PriceTick(string SelectionId, decimal Price);
}
