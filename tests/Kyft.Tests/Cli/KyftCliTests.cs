using System.Runtime.CompilerServices;

using Kyft.Cli;

namespace Kyft.Tests.Cli;

public sealed class KyftCliTests
{
    [Fact]
    public void ValidatePlanAcceptsValidFixture()
    {
        var (exitCode, output, error) = Run("validate-plan", FixturePath("basic-overlap.json"));

        Assert.Equal(0, exitCode);
        Assert.Contains("\"isValid\":true", output);
        Assert.Equal(string.Empty, error);
    }

    [Fact]
    public void CompareRunsFixtureAndWritesJson()
    {
        var (exitCode, output, error) = Run("compare", FixturePath("basic-overlap.json"), "--format", "json");

        Assert.Equal(0, exitCode);
        Assert.Contains("\"schema\": \"kyft.comparison.result\"", output);
        Assert.Contains("\"overlap\"", output);
        Assert.Equal(string.Empty, error);
    }

    [Fact]
    public void ExplainRunsFixtureAndWritesMarkdown()
    {
        var (exitCode, output, error) = Run("explain", FixturePath("basic-overlap.json"));

        Assert.Equal(0, exitCode);
        Assert.Contains("# Comparison Explain: Provider QA", output);
        Assert.Equal(string.Empty, error);
    }

    [Fact]
    public void InvalidInputReturnsNonZeroExitCodeAndDiagnosticOutput()
    {
        var (exitCode, output, error) = Run("compare", "missing-fixture.json");

        Assert.Equal(2, exitCode);
        Assert.Equal(string.Empty, output);
        Assert.Contains("\"error\":", error);
    }

    private static (int ExitCode, string Output, string Error) Run(params string[] args)
    {
        using var output = new StringWriter();
        using var error = new StringWriter();
        var exitCode = KyftCli.Run(args, output, error);
        return (exitCode, output.ToString(), error.ToString());
    }

    private static string FixturePath(string name, [CallerFilePath] string callerFilePath = "")
    {
        return Path.Combine(
            Path.GetDirectoryName(callerFilePath)!,
            "..",
            "Comparison",
            "Fixtures",
            name);
    }
}
