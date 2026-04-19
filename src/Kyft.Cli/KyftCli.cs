using System.Text.Json;

using Kyft.Testing;

namespace Kyft.Cli;

public static class KyftCli
{
    public static int Run(string[] args, TextWriter stdout, TextWriter stderr)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(stdout);
        ArgumentNullException.ThrowIfNull(stderr);

        try
        {
            if (args.Length < 2)
            {
                WriteError(stderr, "Usage: kyft <validate-plan|compare|explain> <fixture.json> [--format json|markdown]");
                return 2;
            }

            var command = args[0];
            var fixturePath = args[1];
            var format = ReadFormat(args);
            using var fixture = JsonDocument.Parse(File.ReadAllText(fixturePath));
            var result = ExecuteFixture(fixture.RootElement);

            if (string.Equals(command, "validate-plan", StringComparison.Ordinal))
            {
                WriteDiagnostics(stdout, result);
                return result.IsValid ? 0 : 1;
            }

            if (string.Equals(command, "compare", StringComparison.Ordinal))
            {
                stdout.Write(format == "markdown" ? result.ExportMarkdown() : result.ExportJson());
                return result.IsValid ? 0 : 1;
            }

            if (string.Equals(command, "explain", StringComparison.Ordinal))
            {
                stdout.Write(result.ExportMarkdown());
                return result.IsValid ? 0 : 1;
            }

            WriteError(stderr, "Unknown command: " + command);
            return 2;
        }
        catch (Exception exception) when (exception is IOException or JsonException or ArgumentException or InvalidOperationException)
        {
            WriteError(stderr, exception.Message);
            return 2;
        }
    }

    private static string ReadFormat(string[] args)
    {
        for (var i = 2; i < args.Length - 1; i++)
        {
            if (string.Equals(args[i], "--format", StringComparison.Ordinal))
            {
                var format = args[i + 1];
                if (string.Equals(format, "json", StringComparison.Ordinal)
                    || string.Equals(format, "markdown", StringComparison.Ordinal))
                {
                    return format;
                }

                throw new ArgumentException("Unsupported format: " + format);
            }
        }

        return "json";
    }

    private static ComparisonResult ExecuteFixture(JsonElement fixture)
    {
        var history = CreateHistory(fixture.GetProperty("windows"));
        var plan = CreatePlan(fixture.GetProperty("plan"));
        var builder = history.Compare(plan.Name)
            .Target(plan.Target!.Value.Name, _ => plan.Target.Value);

        for (var i = 0; i < plan.Against.Count; i++)
        {
            var selector = plan.Against[i];
            builder.Against(selector.Name, _ => selector);
        }

        return builder
            .Within(_ => plan.Scope!)
            .Using(_ => BuildComparators(plan.Comparators))
            .Normalize(_ => BuildNormalization(plan.Normalization))
            .StrictIf(plan.IsStrict)
            .Run();
    }

    private static WindowIntervalHistory CreateHistory(JsonElement windows)
    {
        var builder = new WindowHistoryFixtureBuilder();
        foreach (var window in windows.EnumerateArray())
        {
            builder.AddClosedWindow(
                window.GetProperty("windowName").GetString()!,
                window.GetProperty("key").GetString()!,
                window.GetProperty("startPosition").GetInt64(),
                window.GetProperty("endPosition").GetInt64(),
                source: window.GetProperty("source").GetString());
        }

        return builder.Build();
    }

    private static ComparisonPlan CreatePlan(JsonElement plan)
    {
        var against = plan.GetProperty("againstSources").EnumerateArray()
            .Select(static source => ComparisonSelector.ForSource(source.GetString()!))
            .ToArray();
        var scope = plan.GetProperty("scopeWindow").ValueKind == JsonValueKind.Null
            ? ComparisonScope.All()
            : ComparisonScope.Window(plan.GetProperty("scopeWindow").GetString()!);

        return new ComparisonPlan(
            plan.GetProperty("name").GetString()!,
            ComparisonSelector.ForSource(plan.GetProperty("targetSource").GetString()!),
            against,
            scope,
            ComparisonNormalizationPolicy.Default,
            plan.GetProperty("comparators").EnumerateArray().Select(static comparator => comparator.GetString()!),
            ComparisonOutputOptions.Default,
            plan.GetProperty("strict").GetBoolean());
    }

    private static ComparisonComparatorBuilder BuildComparators(IReadOnlyList<string> comparators)
    {
        var builder = new ComparisonComparatorBuilder();
        for (var i = 0; i < comparators.Count; i++)
        {
            var comparator = comparators[i];
            if (string.Equals(comparator, "overlap", StringComparison.Ordinal))
            {
                builder.Overlap();
            }
            else if (string.Equals(comparator, "residual", StringComparison.Ordinal))
            {
                builder.Residual();
            }
            else if (string.Equals(comparator, "missing", StringComparison.Ordinal))
            {
                builder.Missing();
            }
            else if (string.Equals(comparator, "coverage", StringComparison.Ordinal))
            {
                builder.Coverage();
            }
            else if (string.Equals(comparator, "gap", StringComparison.Ordinal))
            {
                builder.Gap();
            }
            else if (string.Equals(comparator, "symmetric-difference", StringComparison.Ordinal))
            {
                builder.SymmetricDifference();
            }
        }

        return builder;
    }

    private static ComparisonNormalizationBuilder BuildNormalization(ComparisonNormalizationPolicy policy)
    {
        var builder = new ComparisonNormalizationBuilder();
        if (policy.TimeAxis == TemporalAxis.Timestamp)
        {
            builder.OnEventTime();
        }

        if (policy.OpenWindowPolicy == ComparisonOpenWindowPolicy.ClipToHorizon
            && policy.OpenWindowHorizon.HasValue)
        {
            builder.ClipOpenWindowsTo(policy.OpenWindowHorizon.Value);
        }

        return builder;
    }

    private static void WriteDiagnostics(TextWriter writer, ComparisonResult result)
    {
        writer.Write("{\"isValid\":");
        writer.Write(result.IsValid ? "true" : "false");
        writer.Write(",\"diagnostics\":[");
        for (var i = 0; i < result.Diagnostics.Count; i++)
        {
            if (i > 0)
            {
                writer.Write(',');
            }

            writer.Write('"');
            writer.Write(result.Diagnostics[i].Code.ToString());
            writer.Write('"');
        }

        writer.Write("]}");
    }

    private static void WriteError(TextWriter writer, string message)
    {
        writer.Write("{\"error\":");
        writer.Write(JsonSerializer.Serialize(message));
        writer.Write('}');
    }
}

internal static class WindowComparisonBuilderCliExtensions
{
    internal static WindowComparisonBuilder StrictIf(this WindowComparisonBuilder builder, bool isStrict)
    {
        return isStrict ? builder.Strict() : builder;
    }
}
