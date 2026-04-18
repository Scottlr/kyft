using System.Globalization;
using System.Text;
using System.Text.Json;

using Kyft;

namespace Kyft.Internal.Comparison;

internal static class ComparisonExporter
{
    private const string PlanSchema = "kyft.comparison.plan";
    private const string ResultSchema = "kyft.comparison.result";
    private const string RowSchema = "kyft.comparison.result-row";
    private const int SchemaVersion = 0;

    internal static string ExportJson(ComparisonPlan plan)
    {
        EnsureExportable(plan);

        using var stream = new MemoryStream();
        using (var writer = CreateWriter(stream, indented: true))
        {
            WritePlanEnvelope(writer, plan, plan.Validate());
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    internal static string ExportJson(ComparisonResult result)
    {
        EnsureExportable(result.Plan);

        using var stream = new MemoryStream();
        using (var writer = CreateWriter(stream, indented: true))
        {
            WriteResultEnvelope(writer, result);
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    internal static IEnumerable<string> ExportJsonLines(ComparisonResult result)
    {
        EnsureExportable(result.Plan);

        yield return ExportJsonLine(writer => WriteResultLine(writer, result));

        for (var i = 0; i < result.OverlapRows.Count; i++)
        {
            var row = result.OverlapRows[i];
            yield return ExportJsonLine(writer =>
            {
                WriteRowEnvelopeStart(writer, "overlap", i);
                WriteOverlapRowFields(writer, row);
                writer.WriteEndObject();
            });
        }

        for (var i = 0; i < result.ResidualRows.Count; i++)
        {
            var row = result.ResidualRows[i];
            yield return ExportJsonLine(writer =>
            {
                WriteRowEnvelopeStart(writer, "residual", i);
                WriteResidualRowFields(writer, row);
                writer.WriteEndObject();
            });
        }

        for (var i = 0; i < result.MissingRows.Count; i++)
        {
            var row = result.MissingRows[i];
            yield return ExportJsonLine(writer =>
            {
                WriteRowEnvelopeStart(writer, "missing", i);
                WriteMissingRowFields(writer, row);
                writer.WriteEndObject();
            });
        }

        for (var i = 0; i < result.CoverageRows.Count; i++)
        {
            var row = result.CoverageRows[i];
            yield return ExportJsonLine(writer =>
            {
                WriteRowEnvelopeStart(writer, "coverage", i);
                WriteCoverageRowFields(writer, row);
                writer.WriteEndObject();
            });
        }
    }

    private static void EnsureExportable(ComparisonPlan plan)
    {
        if (plan.IsSerializable)
        {
            return;
        }

        var diagnostics = plan.Validate()
            .Where(static diagnostic => diagnostic.Code == ComparisonPlanValidationCode.NonSerializableSelector)
            .ToArray();

        if (diagnostics.Length == 0)
        {
            diagnostics =
            [
                new ComparisonPlanDiagnostic(
                    ComparisonPlanValidationCode.NonSerializableSelector,
                    "Comparison plan contains runtime-only selectors and cannot be exported as portable data.",
                    "selectors",
                    ComparisonPlanDiagnosticSeverity.Error)
            ];
        }

        throw new ComparisonExportException(
            "Comparison plan contains runtime-only selectors and cannot be exported as portable data.",
            diagnostics);
    }

    private static Utf8JsonWriter CreateWriter(Stream stream, bool indented)
    {
        return new Utf8JsonWriter(stream, new JsonWriterOptions
        {
            Indented = indented
        });
    }

    private static string ExportJsonLine(Action<Utf8JsonWriter> write)
    {
        using var stream = new MemoryStream();
        using (var writer = CreateWriter(stream, indented: false))
        {
            write(writer);
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void WritePlanEnvelope(
        Utf8JsonWriter writer,
        ComparisonPlan plan,
        IReadOnlyList<ComparisonPlanDiagnostic> diagnostics)
    {
        writer.WriteStartObject();
        writer.WriteString("schema", PlanSchema);
        writer.WriteNumber("schemaVersion", SchemaVersion);
        writer.WriteString("artifact", "plan");
        WritePlanFields(writer, plan, diagnostics);
        writer.WriteEndObject();
    }

    private static void WriteResultEnvelope(Utf8JsonWriter writer, ComparisonResult result)
    {
        writer.WriteStartObject();
        writer.WriteString("schema", ResultSchema);
        writer.WriteNumber("schemaVersion", SchemaVersion);
        writer.WriteString("artifact", "result");
        writer.WriteBoolean("isValid", result.IsValid);
        writer.WritePropertyName("plan");
        writer.WriteStartObject();
        WritePlanFields(writer, result.Plan, result.Diagnostics);
        writer.WriteEndObject();
        WriteDiagnostics(writer, "diagnostics", result.Diagnostics);
        WritePrepared(writer, result.Prepared);
        WriteAligned(writer, result.Aligned);
        WriteComparatorSummaries(writer, result.ComparatorSummaries);
        WriteRows(writer, result);
        WriteCoverageSummaries(writer, result.CoverageSummaries);
        writer.WriteEndObject();
    }

    private static void WriteResultLine(Utf8JsonWriter writer, ComparisonResult result)
    {
        writer.WriteStartObject();
        writer.WriteString("schema", RowSchema);
        writer.WriteNumber("schemaVersion", SchemaVersion);
        writer.WriteString("artifact", "result-summary");
        writer.WriteString("planName", result.Plan.Name);
        writer.WriteBoolean("isValid", result.IsValid);
        writer.WriteNumber("diagnosticCount", result.Diagnostics.Count);
        writer.WriteNumber("overlapRowCount", result.OverlapRows.Count);
        writer.WriteNumber("residualRowCount", result.ResidualRows.Count);
        writer.WriteNumber("missingRowCount", result.MissingRows.Count);
        writer.WriteNumber("coverageRowCount", result.CoverageRows.Count);
        writer.WriteEndObject();
    }

    private static void WriteRowEnvelopeStart(Utf8JsonWriter writer, string rowType, int index)
    {
        writer.WriteStartObject();
        writer.WriteString("schema", RowSchema);
        writer.WriteNumber("schemaVersion", SchemaVersion);
        writer.WriteString("artifact", "result-row");
        writer.WriteString("rowType", rowType);
        writer.WriteString("rowId", rowType + "[" + index.ToString(CultureInfo.InvariantCulture) + "]");
    }

    private static void WritePlanFields(
        Utf8JsonWriter writer,
        ComparisonPlan plan,
        IReadOnlyList<ComparisonPlanDiagnostic> diagnostics)
    {
        writer.WriteString("name", plan.Name);
        writer.WriteBoolean("isStrict", plan.IsStrict);
        writer.WriteBoolean("isSerializable", plan.IsSerializable);
        writer.WritePropertyName("target");
        if (plan.Target.HasValue)
        {
            WriteSelector(writer, plan.Target.Value);
        }
        else
        {
            writer.WriteNullValue();
        }

        writer.WritePropertyName("against");
        writer.WriteStartArray();
        for (var i = 0; i < plan.Against.Count; i++)
        {
            WriteSelector(writer, plan.Against[i]);
        }

        writer.WriteEndArray();
        writer.WritePropertyName("scope");
        WriteScope(writer, plan.Scope);
        writer.WritePropertyName("normalization");
        WriteNormalization(writer, plan.Normalization);
        WriteStringArray(writer, "comparators", plan.Comparators);
        writer.WritePropertyName("output");
        WriteOutput(writer, plan.Output);
        WriteDiagnostics(writer, "diagnostics", diagnostics);
    }

    private static void WriteSelector(Utf8JsonWriter writer, ComparisonSelector selector)
    {
        writer.WriteStartObject();
        writer.WriteString("name", selector.Name);
        writer.WriteString("description", selector.Description);
        writer.WriteBoolean("isSerializable", selector.IsSerializable);
        writer.WriteEndObject();
    }

    private static void WriteScope(Utf8JsonWriter writer, ComparisonScope? scope)
    {
        if (scope is null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStartObject();
        WriteNullableString(writer, "windowName", scope.WindowName);
        writer.WriteString("timeAxis", scope.TimeAxis.ToString());
        writer.WriteEndObject();
    }

    private static void WriteNormalization(Utf8JsonWriter writer, ComparisonNormalizationPolicy policy)
    {
        writer.WriteStartObject();
        writer.WriteBoolean("requireClosedWindows", policy.RequireClosedWindows);
        writer.WriteBoolean("useHalfOpenRanges", policy.UseHalfOpenRanges);
        writer.WriteString("timeAxis", policy.TimeAxis.ToString());
        writer.WriteString("openWindowPolicy", policy.OpenWindowPolicy.ToString());
        writer.WritePropertyName("openWindowHorizon");
        WritePoint(writer, policy.OpenWindowHorizon);
        writer.WriteString("nullTimestampPolicy", policy.NullTimestampPolicy.ToString());
        writer.WriteBoolean("coalesceAdjacentWindows", policy.CoalesceAdjacentWindows);
        writer.WriteString("duplicateWindowPolicy", policy.DuplicateWindowPolicy.ToString());
        writer.WriteEndObject();
    }

    private static void WriteOutput(Utf8JsonWriter writer, ComparisonOutputOptions output)
    {
        writer.WriteStartObject();
        writer.WriteBoolean("includeAlignedSegments", output.IncludeAlignedSegments);
        writer.WriteBoolean("includeExplainData", output.IncludeExplainData);
        writer.WriteEndObject();
    }

    private static void WriteDiagnostics(
        Utf8JsonWriter writer,
        string propertyName,
        IReadOnlyList<ComparisonPlanDiagnostic> diagnostics)
    {
        writer.WritePropertyName(propertyName);
        writer.WriteStartArray();
        for (var i = 0; i < diagnostics.Count; i++)
        {
            var diagnostic = diagnostics[i];
            writer.WriteStartObject();
            writer.WriteString("code", diagnostic.Code.ToString());
            writer.WriteString("message", diagnostic.Message);
            writer.WriteString("path", diagnostic.Path);
            writer.WriteString("severity", diagnostic.Severity.ToString());
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    private static void WritePrepared(Utf8JsonWriter writer, PreparedComparison? prepared)
    {
        writer.WritePropertyName("prepared");
        if (prepared is null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStartObject();
        writer.WritePropertyName("selectedWindows");
        writer.WriteStartArray();
        for (var i = 0; i < prepared.SelectedWindows.Count; i++)
        {
            WriteWindow(writer, prepared.SelectedWindows[i]);
        }

        writer.WriteEndArray();
        writer.WritePropertyName("excludedWindows");
        writer.WriteStartArray();
        for (var i = 0; i < prepared.ExcludedWindows.Count; i++)
        {
            var excluded = prepared.ExcludedWindows[i];
            writer.WriteStartObject();
            writer.WriteString("recordId", excluded.Window.Id.ToString());
            writer.WriteString("reason", excluded.Reason);
            WriteNullableString(writer, "diagnosticCode", excluded.DiagnosticCode?.ToString());
            writer.WritePropertyName("window");
            WriteWindow(writer, excluded.Window);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WritePropertyName("normalizedWindows");
        writer.WriteStartArray();
        for (var i = 0; i < prepared.NormalizedWindows.Count; i++)
        {
            var normalized = prepared.NormalizedWindows[i];
            writer.WriteStartObject();
            writer.WriteString("recordId", normalized.RecordId.ToString());
            writer.WriteString("selectorName", normalized.SelectorName);
            writer.WriteString("side", normalized.Side.ToString());
            writer.WritePropertyName("range");
            WriteRange(writer, normalized.Range);
            writer.WritePropertyName("window");
            WriteWindow(writer, normalized.Window);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private static void WriteAligned(Utf8JsonWriter writer, AlignedComparison? aligned)
    {
        writer.WritePropertyName("aligned");
        if (aligned is null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStartObject();
        writer.WritePropertyName("segments");
        writer.WriteStartArray();
        for (var i = 0; i < aligned.Segments.Count; i++)
        {
            var segment = aligned.Segments[i];
            writer.WriteStartObject();
            writer.WriteString("segmentId", "segment[" + i.ToString(CultureInfo.InvariantCulture) + "]");
            writer.WriteString("windowName", segment.WindowName);
            WriteObjectValue(writer, "key", segment.Key);
            WriteObjectValue(writer, "partition", segment.Partition);
            writer.WritePropertyName("range");
            WriteRange(writer, segment.Range);
            WriteIds(writer, "targetRecordIds", segment.TargetRecordIds);
            WriteIds(writer, "againstRecordIds", segment.AgainstRecordIds);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private static void WriteComparatorSummaries(
        Utf8JsonWriter writer,
        IReadOnlyList<ComparatorSummary> summaries)
    {
        writer.WritePropertyName("comparatorSummaries");
        writer.WriteStartArray();
        for (var i = 0; i < summaries.Count; i++)
        {
            var summary = summaries[i];
            writer.WriteStartObject();
            writer.WriteString("comparatorName", summary.ComparatorName);
            writer.WriteNumber("rowCount", summary.RowCount);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    private static void WriteRows(Utf8JsonWriter writer, ComparisonResult result)
    {
        writer.WritePropertyName("rows");
        writer.WriteStartObject();
        writer.WritePropertyName("overlap");
        writer.WriteStartArray();
        for (var i = 0; i < result.OverlapRows.Count; i++)
        {
            WriteRowObject(writer, "overlap", i, () => WriteOverlapRowFields(writer, result.OverlapRows[i]));
        }

        writer.WriteEndArray();
        writer.WritePropertyName("residual");
        writer.WriteStartArray();
        for (var i = 0; i < result.ResidualRows.Count; i++)
        {
            WriteRowObject(writer, "residual", i, () => WriteResidualRowFields(writer, result.ResidualRows[i]));
        }

        writer.WriteEndArray();
        writer.WritePropertyName("missing");
        writer.WriteStartArray();
        for (var i = 0; i < result.MissingRows.Count; i++)
        {
            WriteRowObject(writer, "missing", i, () => WriteMissingRowFields(writer, result.MissingRows[i]));
        }

        writer.WriteEndArray();
        writer.WritePropertyName("coverage");
        writer.WriteStartArray();
        for (var i = 0; i < result.CoverageRows.Count; i++)
        {
            WriteRowObject(writer, "coverage", i, () => WriteCoverageRowFields(writer, result.CoverageRows[i]));
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private static void WriteRowObject(Utf8JsonWriter writer, string rowType, int index, Action writeFields)
    {
        writer.WriteStartObject();
        writer.WriteString("rowId", rowType + "[" + index.ToString(CultureInfo.InvariantCulture) + "]");
        writeFields();
        writer.WriteEndObject();
    }

    private static void WriteOverlapRowFields(Utf8JsonWriter writer, OverlapRow row)
    {
        WriteCommonRowFields(writer, row.WindowName, row.Key, row.Partition, row.Range);
        WriteIds(writer, "targetRecordIds", row.TargetRecordIds);
        WriteIds(writer, "againstRecordIds", row.AgainstRecordIds);
    }

    private static void WriteResidualRowFields(Utf8JsonWriter writer, ResidualRow row)
    {
        WriteCommonRowFields(writer, row.WindowName, row.Key, row.Partition, row.Range);
        WriteIds(writer, "targetRecordIds", row.TargetRecordIds);
    }

    private static void WriteMissingRowFields(Utf8JsonWriter writer, MissingRow row)
    {
        WriteCommonRowFields(writer, row.WindowName, row.Key, row.Partition, row.Range);
        WriteIds(writer, "againstRecordIds", row.AgainstRecordIds);
    }

    private static void WriteCoverageRowFields(Utf8JsonWriter writer, CoverageRow row)
    {
        WriteCommonRowFields(writer, row.WindowName, row.Key, row.Partition, row.Range);
        writer.WriteNumber("targetMagnitude", row.TargetMagnitude);
        writer.WriteNumber("coveredMagnitude", row.CoveredMagnitude);
        WriteIds(writer, "targetRecordIds", row.TargetRecordIds);
        WriteIds(writer, "againstRecordIds", row.AgainstRecordIds);
    }

    private static void WriteCommonRowFields(
        Utf8JsonWriter writer,
        string windowName,
        object key,
        object? partition,
        TemporalRange range)
    {
        writer.WriteString("windowName", windowName);
        WriteObjectValue(writer, "key", key);
        WriteObjectValue(writer, "partition", partition);
        writer.WritePropertyName("range");
        WriteRange(writer, range);
    }

    private static void WriteCoverageSummaries(
        Utf8JsonWriter writer,
        IReadOnlyList<CoverageSummary> summaries)
    {
        writer.WritePropertyName("coverageSummaries");
        writer.WriteStartArray();
        for (var i = 0; i < summaries.Count; i++)
        {
            var summary = summaries[i];
            writer.WriteStartObject();
            writer.WriteString("windowName", summary.WindowName);
            WriteObjectValue(writer, "key", summary.Key);
            WriteObjectValue(writer, "partition", summary.Partition);
            writer.WriteNumber("targetMagnitude", summary.TargetMagnitude);
            writer.WriteNumber("coveredMagnitude", summary.CoveredMagnitude);
            writer.WriteNumber("coverageRatio", summary.CoverageRatio);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    private static void WriteWindow(Utf8JsonWriter writer, WindowRecord window)
    {
        writer.WriteStartObject();
        writer.WriteString("recordId", window.Id.ToString());
        writer.WriteString("windowName", window.WindowName);
        WriteObjectValue(writer, "key", window.Key);
        WriteObjectValue(writer, "source", window.Source);
        WriteObjectValue(writer, "partition", window.Partition);
        writer.WriteNumber("startPosition", window.StartPosition);
        WriteNullableNumber(writer, "endPosition", window.EndPosition);
        WriteNullableTimestamp(writer, "startTime", window.StartTime);
        WriteNullableTimestamp(writer, "endTime", window.EndTime);
        writer.WriteBoolean("isClosed", window.IsClosed);
        writer.WriteEndObject();
    }

    private static void WriteRange(Utf8JsonWriter writer, TemporalRange range)
    {
        writer.WriteStartObject();
        writer.WriteString("axis", range.Axis.ToString());
        writer.WritePropertyName("start");
        WritePoint(writer, range.Start);
        writer.WritePropertyName("end");
        WritePoint(writer, range.End);
        writer.WriteString("endStatus", range.EndStatus.ToString());
        writer.WriteEndObject();
    }

    private static void WritePoint(Utf8JsonWriter writer, TemporalPoint? point)
    {
        if (!point.HasValue)
        {
            writer.WriteNullValue();
            return;
        }

        var value = point.Value;
        writer.WriteStartObject();
        writer.WriteString("axis", value.Axis.ToString());
        if (value.Axis == TemporalAxis.ProcessingPosition)
        {
            writer.WriteNumber("position", value.Position);
        }
        else if (value.Axis == TemporalAxis.Timestamp)
        {
            writer.WriteString("timestamp", value.Timestamp.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
            WriteNullableString(writer, "clock", value.Clock);
        }

        writer.WriteEndObject();
    }

    private static void WriteObjectValue(Utf8JsonWriter writer, string propertyName, object? value)
    {
        writer.WritePropertyName(propertyName);
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStartObject();
        writer.WriteString("type", value.GetType().FullName);
        writer.WriteString("value", StableObjectText(value));
        writer.WriteEndObject();
    }

    private static void WriteStringArray(
        Utf8JsonWriter writer,
        string propertyName,
        IReadOnlyList<string> values)
    {
        writer.WritePropertyName(propertyName);
        writer.WriteStartArray();
        for (var i = 0; i < values.Count; i++)
        {
            writer.WriteStringValue(values[i]);
        }

        writer.WriteEndArray();
    }

    private static void WriteIds(
        Utf8JsonWriter writer,
        string propertyName,
        IReadOnlyList<WindowRecordId> ids)
    {
        writer.WritePropertyName(propertyName);
        writer.WriteStartArray();
        for (var i = 0; i < ids.Count; i++)
        {
            writer.WriteStringValue(ids[i].ToString());
        }

        writer.WriteEndArray();
    }

    private static void WriteNullableString(Utf8JsonWriter writer, string propertyName, string? value)
    {
        writer.WritePropertyName(propertyName);
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStringValue(value);
    }

    private static void WriteNullableNumber(Utf8JsonWriter writer, string propertyName, long? value)
    {
        writer.WritePropertyName(propertyName);
        if (value.HasValue)
        {
            writer.WriteNumberValue(value.Value);
            return;
        }

        writer.WriteNullValue();
    }

    private static void WriteNullableTimestamp(Utf8JsonWriter writer, string propertyName, DateTimeOffset? value)
    {
        writer.WritePropertyName(propertyName);
        if (value.HasValue)
        {
            writer.WriteStringValue(value.Value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
            return;
        }

        writer.WriteNullValue();
    }

    private static string StableObjectText(object value)
    {
        return value switch
        {
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };
    }
}
