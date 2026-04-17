using Kyft;

namespace Kyft.Tests.Api;

public sealed class PublicEntryPointTests
{
    [Fact]
    public void ForReturnsBuilderForEventType()
    {
        var builder = Kyft.For<SampleEvent>();

        Assert.IsType<EventPipelineBuilder<SampleEvent>>(builder);
    }

    private sealed record SampleEvent;
}
