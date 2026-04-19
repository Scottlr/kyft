using Kyft;

namespace Kyft.Tests.Api;

public sealed class ChildActivityViewTests
{
    [Fact]
    public void EmptyChildrenAreNotAllActive()
    {
        var children = new ChildActivityView(activeCount: 0, totalCount: 0);

        Assert.False(children.AllActive());
        Assert.False(children.AnyActive());
        Assert.Equal(0, children.ActiveCount);
        Assert.Equal(0, children.TotalCount);
    }

    [Fact]
    public void PartialChildrenAreAnyActiveButNotAllActive()
    {
        var children = new ChildActivityView(activeCount: 1, totalCount: 2);

        Assert.False(children.AllActive());
        Assert.True(children.AnyActive());
        Assert.Equal(1, children.ActiveCount);
        Assert.Equal(2, children.TotalCount);
    }

    [Fact]
    public void CompleteChildrenAreAllActiveAndAnyActive()
    {
        var children = new ChildActivityView(activeCount: 2, totalCount: 2);

        Assert.True(children.AllActive());
        Assert.True(children.AnyActive());
    }

    [Fact]
    public void ChildActivityViewCanBeDeconstructed()
    {
        var children = new ChildActivityView(activeCount: 1, totalCount: 3);

        var (activeCount, totalCount) = children;

        Assert.Equal(1, activeCount);
        Assert.Equal(3, totalCount);
    }
}
