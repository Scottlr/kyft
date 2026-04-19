using System.Text.Json;

using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class ComparisonExtensionTests
{
    [Fact]
    public void ExperimentalExtensionCanRegisterComparatorAndSelector()
    {
        var descriptor = ExperimentalOddsExtension.Describe();

        Assert.Equal("experimental-odds", descriptor.Id);
        Assert.Contains(descriptor.Comparators, comparator =>
            comparator.Declaration == "odds:edge");
        Assert.Contains(descriptor.Selectors, selector =>
            selector.Name == "market");
    }

    [Fact]
    public void CoreVocabularyRemainsDomainNeutral()
    {
        var coreTypeNames = typeof(ComparisonResult).Assembly
            .GetTypes()
            .Select(static type => type.FullName ?? string.Empty)
            .ToArray();

        Assert.DoesNotContain(coreTypeNames, name => name.Contains("Odds", StringComparison.Ordinal));
        Assert.DoesNotContain(coreTypeNames, name => name.Contains("Bookmaker", StringComparison.Ordinal));
    }

    [Fact]
    public void ResultExportHandlesExtensionMetadata()
    {
        var result = new ComparisonResult(
            new ComparisonPlan(
                "Extension QA",
                ComparisonSelector.ForSource("provider-a"),
                [ComparisonSelector.ForSource("provider-b")],
                ComparisonScope.Window("DeviceOffline"),
                ComparisonNormalizationPolicy.Default,
                ["overlap"],
                ComparisonOutputOptions.Default),
            [],
            extensionMetadata:
            [
                new ComparisonExtensionMetadata("experimental-odds", "edgeThreshold", "0.025")
            ]);

        using var document = JsonDocument.Parse(result.ExportJson());
        var metadata = Assert.Single(document.RootElement.GetProperty("extensionMetadata").EnumerateArray());

        Assert.Equal("experimental-odds", metadata.GetProperty("extensionId").GetString());
        Assert.Equal("edgeThreshold", metadata.GetProperty("key").GetString());
        Assert.Contains("extensionMetadata[0]: experimental-odds.edgeThreshold=0.025", result.Explain());
    }

    private static class ExperimentalOddsExtension
    {
        internal static ComparisonExtensionDescriptor Describe()
        {
            return new ComparisonExtensionBuilder("experimental-odds", "Experimental Odds")
                .AddSelector("market", "Selects a market-scoped window.")
                .AddComparator("odds:edge", "Compares expected edge windows.")
                .AddMetadataKey("edgeThreshold")
                .Build();
        }
    }
}
