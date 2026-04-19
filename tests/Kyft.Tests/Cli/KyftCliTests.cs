using System.Runtime.CompilerServices;
using System.Text.Json;

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
    public void CompareHonorsEveryAgainstSourceInFixturePlan()
    {
        var fixturePath = TempFixturePath();
        try
        {
            File.WriteAllText(fixturePath, """
                {
                  "schema": "kyft.contract-fixture",
                  "schemaVersion": 1,
                  "name": "multi-source-overlap",
                  "windows": [
                    {
                      "windowName": "DeviceOffline",
                      "key": "device-1",
                      "source": "provider-a",
                      "startPosition": 1,
                      "endPosition": 5
                    },
                    {
                      "windowName": "DeviceOffline",
                      "key": "device-1",
                      "source": "provider-b",
                      "startPosition": 3,
                      "endPosition": 7
                    },
                    {
                      "windowName": "DeviceOffline",
                      "key": "device-1",
                      "source": "provider-c",
                      "startPosition": 2,
                      "endPosition": 4
                    }
                  ],
                  "plan": {
                    "name": "Provider QA",
                    "targetSource": "provider-a",
                    "againstSources": [ "provider-b", "provider-c" ],
                    "scopeWindow": "DeviceOffline",
                    "comparators": [ "overlap" ],
                    "strict": false
                  }
                }
                """);

            var (exitCode, output, error) = Run("compare", fixturePath, "--format", "json");

            Assert.Equal(0, exitCode);
            Assert.Equal(string.Empty, error);
            using var document = JsonDocument.Parse(output);
            var against = document.RootElement
                .GetProperty("plan")
                .GetProperty("against");
            Assert.Equal(2, against.GetArrayLength());
            Assert.Equal("source:provider-b", against[0].GetProperty("name").GetString());
            Assert.Equal("source:provider-c", against[1].GetProperty("name").GetString());
        }
        finally
        {
            File.Delete(fixturePath);
        }
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

    private static string TempFixturePath()
    {
        return Path.Combine(Path.GetTempPath(), "kyft-cli-" + Guid.NewGuid().ToString("N") + ".json");
    }
}
