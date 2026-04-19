using System.Globalization;
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
            if (!IsKnownCommand(command))
            {
                WriteError(stderr, "Unknown command: " + command);
                return 2;
            }

            var fixturePath = args[1];
            var format = ReadFormat(args);
            using var fixture = JsonDocument.Parse(File.ReadAllText(fixturePath));
            ValidateFixture(fixture.RootElement);
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

            return 2;
        }
        catch (Exception exception) when (exception is IOException or JsonException or ArgumentException or InvalidOperationException)
        {
            WriteError(stderr, exception.Message);
            return 2;
        }
    }

    private static bool IsKnownCommand(string command)
    {
        return string.Equals(command, "validate-plan", StringComparison.Ordinal)
            || string.Equals(command, "compare", StringComparison.Ordinal)
            || string.Equals(command, "explain", StringComparison.Ordinal);
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
        var liveHorizon = ReadLiveHorizon(fixture.GetProperty("plan"));
        var builder = history.Compare(plan.Name)
            .Target(plan.Target!.Value.Name, _ => plan.Target.Value);

        for (var i = 0; i < plan.Against.Count; i++)
        {
            var selector = plan.Against[i];
            builder.Against(selector.Name, _ => selector);
        }

        builder = builder
            .Within(_ => plan.Scope!)
            .Using(_ => BuildComparators(plan.Comparators))
            .Normalize(_ => BuildNormalization(plan.Normalization))
            .StrictIf(plan.IsStrict);

        return liveHorizon.HasValue
            ? builder.RunLive(liveHorizon.Value)
            : builder.Run();
    }

    private static WindowIntervalHistory CreateHistory(JsonElement windows)
    {
        var builder = new WindowHistoryFixtureBuilder();
        foreach (var window in windows.EnumerateArray())
        {
            var windowName = window.GetProperty("windowName").GetString()!;
            var key = window.GetProperty("key").GetString()!;
            var startPosition = window.GetProperty("startPosition").GetInt64();
            var source = window.GetProperty("source").GetString();
            var partition = window.TryGetProperty("partition", out var partitionProperty)
                && partitionProperty.ValueKind != JsonValueKind.Null
                    ? partitionProperty.GetString()
                    : null;

            if (window.GetProperty("endPosition").ValueKind == JsonValueKind.Null)
            {
                builder.AddOpenWindow(windowName, key, startPosition, source, partition);
                continue;
            }

            builder.AddClosedWindow(
                windowName,
                key,
                startPosition,
                window.GetProperty("endPosition").GetInt64(),
                source,
                partition);
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

    private static TemporalPoint? ReadLiveHorizon(JsonElement plan)
    {
        return plan.TryGetProperty("liveHorizonPosition", out var horizon)
            && horizon.ValueKind != JsonValueKind.Null
                ? TemporalPoint.ForPosition(horizon.GetInt64())
                : null;
    }

    private static ComparisonComparatorBuilder BuildComparators(IReadOnlyList<string> comparators)
    {
        var builder = new ComparisonComparatorBuilder();
        for (var i = 0; i < comparators.Count; i++)
        {
            builder.Declaration(comparators[i]);
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

    private static void ValidateFixture(JsonElement fixture)
    {
        RequireKind(fixture, "$", JsonValueKind.Object);
        RequireProperty(fixture, "schema", "$", JsonValueKind.String);
        RequireProperty(fixture, "schemaVersion", "$", JsonValueKind.Number);
        var windows = RequireProperty(fixture, "windows", "$", JsonValueKind.Array);
        var plan = RequireProperty(fixture, "plan", "$", JsonValueKind.Object);

        ValidateWindows(windows);
        ValidatePlan(plan);
    }

    private static void ValidateWindows(JsonElement windows)
    {
        var index = 0;
        foreach (var window in windows.EnumerateArray())
        {
            var path = "$.windows[" + index.ToString(CultureInfo.InvariantCulture) + "]";
            RequireKind(window, path, JsonValueKind.Object);
            RequireProperty(window, "windowName", path, JsonValueKind.String);
            RequireProperty(window, "key", path, JsonValueKind.String);
            RequireProperty(window, "source", path, JsonValueKind.String);
            var start = RequireProperty(window, "startPosition", path, JsonValueKind.Number).GetInt64();
            var end = RequireProperty(window, "endPosition", path, JsonValueKind.Number, JsonValueKind.Null);

            if (end.ValueKind == JsonValueKind.Number && end.GetInt64() < start)
            {
                throw new ArgumentException(path + ".endPosition must be greater than or equal to startPosition.");
            }

            if (window.TryGetProperty("partition", out var partition))
            {
                RequireKind(partition, path + ".partition", JsonValueKind.String, JsonValueKind.Null);
            }

            index++;
        }
    }

    private static void ValidatePlan(JsonElement plan)
    {
        RequireProperty(plan, "name", "$.plan", JsonValueKind.String);
        RequireProperty(plan, "targetSource", "$.plan", JsonValueKind.String);
        var against = RequireProperty(plan, "againstSources", "$.plan", JsonValueKind.Array);
        if (against.GetArrayLength() == 0)
        {
            throw new ArgumentException("$.plan.againstSources must contain at least one source.");
        }

        var againstIndex = 0;
        foreach (var source in against.EnumerateArray())
        {
            RequireKind(
                source,
                "$.plan.againstSources[" + againstIndex.ToString(CultureInfo.InvariantCulture) + "]",
                JsonValueKind.String);
            againstIndex++;
        }

        RequireProperty(plan, "scopeWindow", "$.plan", JsonValueKind.String, JsonValueKind.Null);
        var comparators = RequireProperty(plan, "comparators", "$.plan", JsonValueKind.Array);
        if (comparators.GetArrayLength() == 0)
        {
            throw new ArgumentException("$.plan.comparators must contain at least one comparator.");
        }

        var comparatorIndex = 0;
        foreach (var comparator in comparators.EnumerateArray())
        {
            RequireKind(
                comparator,
                "$.plan.comparators[" + comparatorIndex.ToString(CultureInfo.InvariantCulture) + "]",
                JsonValueKind.String);
            comparatorIndex++;
        }

        RequireProperty(plan, "strict", "$.plan", JsonValueKind.True, JsonValueKind.False);
        if (plan.TryGetProperty("liveHorizonPosition", out var horizon))
        {
            RequireKind(horizon, "$.plan.liveHorizonPosition", JsonValueKind.Number, JsonValueKind.Null);
        }
    }

    private static JsonElement RequireProperty(
        JsonElement element,
        string propertyName,
        string path,
        params JsonValueKind[] expectedKinds)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            throw new ArgumentException(path + " is missing required property '" + propertyName + "'.");
        }

        RequireKind(property, path + "." + propertyName, expectedKinds);
        return property;
    }

    private static void RequireKind(JsonElement element, string path, params JsonValueKind[] expectedKinds)
    {
        for (var i = 0; i < expectedKinds.Length; i++)
        {
            if (element.ValueKind == expectedKinds[i])
            {
                return;
            }
        }

        throw new ArgumentException(path + " has unsupported JSON kind " + element.ValueKind + ".");
    }
}

internal static class WindowComparisonBuilderCliExtensions
{
    internal static WindowComparisonBuilder StrictIf(this WindowComparisonBuilder builder, bool isStrict)
    {
        return isStrict ? builder.Strict() : builder;
    }
}
