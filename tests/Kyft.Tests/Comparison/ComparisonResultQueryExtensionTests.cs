using Kyft;

namespace Kyft.Tests.Comparison;

public sealed class ComparisonResultQueryExtensionTests
{
    [Fact]
    public void DiagnosticHelpersReturnDiagnosticsBySeverity()
    {
        var result = new ComparisonResult(
            CreatePlan(),
            [
                new ComparisonPlanDiagnostic(
                    ComparisonPlanValidationCode.UnknownComparator,
                    "Unknown.",
                    "comparators[0]",
                    ComparisonPlanDiagnosticSeverity.Warning),
                new ComparisonPlanDiagnostic(
                    ComparisonPlanValidationCode.MissingTarget,
                    "Missing.",
                    "target",
                    ComparisonPlanDiagnosticSeverity.Error)
            ]);

        Assert.Single(result.WarningDiagnostics());
        Assert.Single(result.ErrorDiagnostics());
    }

    [Fact]
    public void FinalityHelpersReturnRowsByFinality()
    {
        var result = new ComparisonResult(
            CreatePlan(),
            [],
            rowFinalities:
            [
                new ComparisonRowFinality("residual", "residual[0]", ComparisonFinality.Provisional, "open"),
                new ComparisonRowFinality("overlap", "overlap[0]", ComparisonFinality.Final, "closed")
            ]);

        Assert.True(result.HasProvisionalRows());
        Assert.Single(result.ProvisionalRowFinalities());
        Assert.Single(result.FinalRowFinalities());
    }

    private static ComparisonPlan CreatePlan()
    {
        return new ComparisonPlan(
            "Provider QA",
            ComparisonSelector.ForSource("provider-a"),
            [ComparisonSelector.ForSource("provider-b")],
            ComparisonScope.Window("DeviceOffline"),
            ComparisonNormalizationPolicy.Default,
            ["overlap"],
            ComparisonOutputOptions.Default);
    }
}
