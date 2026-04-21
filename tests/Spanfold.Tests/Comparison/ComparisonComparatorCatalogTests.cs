using Spanfold;

namespace Spanfold.Tests.Comparison;

public sealed class ComparisonComparatorCatalogTests
{
    [Fact]
    public void CatalogListsBuiltInComparatorDeclarations()
    {
        Assert.Contains("overlap", ComparisonComparatorCatalog.BuiltInDeclarations);
        Assert.Contains("containment", ComparisonComparatorCatalog.BuiltInDeclarations);
    }

    [Theory]
    [InlineData("overlap")]
    [InlineData("lead-lag:Start:ProcessingPosition:5")]
    [InlineData("asof:Previous:ProcessingPosition:10")]
    public void KnownDeclarationsIncludeParameterizedCoreComparators(string declaration)
    {
        Assert.True(ComparisonComparatorCatalog.IsKnownDeclaration(declaration));
    }

    [Theory]
    [InlineData("lead-lag:Start:Unknown:5")]
    [InlineData("asof:Previous:ProcessingPosition:-1")]
    [InlineData("quality:drift")]
    public void UnknownDeclarationsAreNotClaimedByCoreCatalog(string declaration)
    {
        Assert.False(ComparisonComparatorCatalog.IsKnownDeclaration(declaration));
    }
}
