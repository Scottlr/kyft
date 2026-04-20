using System.Globalization;

namespace Kyft;

/// <summary>
/// Provides typed access to cohort evidence emitted in comparison metadata.
/// </summary>
public static class CohortEvidenceMetadataExtensions
{
    private const string CohortExtensionId = "kyft.cohort";

    /// <summary>
    /// Gets parsed cohort evidence metadata from a comparison result.
    /// </summary>
    /// <param name="result">The comparison result.</param>
    /// <returns>Parsed cohort evidence in result metadata order.</returns>
    public static IReadOnlyList<CohortEvidenceMetadata> CohortEvidence(this ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        var evidence = new List<CohortEvidenceMetadata>();
        for (var i = 0; i < result.ExtensionMetadata.Count; i++)
        {
            var item = result.ExtensionMetadata[i];
            if (!string.Equals(item.ExtensionId, CohortExtensionId, StringComparison.Ordinal))
            {
                continue;
            }

            if (TryParse(item, out var parsed))
            {
                evidence.Add(parsed);
            }
        }

        return evidence.ToArray();
    }

    private static bool TryParse(
        ComparisonExtensionMetadata metadata,
        out CohortEvidenceMetadata evidence)
    {
        evidence = default!;

        if (!TryParseSegmentIndex(metadata.Key, out var segmentIndex))
        {
            return false;
        }

        var values = ParseFields(metadata.Value);
        if (!values.TryGetValue("rule", out var rule)
            || !values.TryGetValue("required", out var required)
            || !values.TryGetValue("activeCount", out var activeCount)
            || !values.TryGetValue("isActive", out var isActive)
            || !int.TryParse(required, NumberStyles.Integer, CultureInfo.InvariantCulture, out var requiredValue)
            || !int.TryParse(activeCount, NumberStyles.Integer, CultureInfo.InvariantCulture, out var activeCountValue)
            || !bool.TryParse(isActive, out var isActiveValue))
        {
            return false;
        }

        evidence = new CohortEvidenceMetadata(
            segmentIndex,
            rule,
            requiredValue,
            activeCountValue,
            isActiveValue,
            ParseActiveSources(values.TryGetValue("activeSources", out var sources) ? sources : string.Empty),
            metadata.Value);
        return true;
    }

    private static bool TryParseSegmentIndex(string key, out int index)
    {
        const string prefix = "segment[";
        index = 0;

        if (!key.StartsWith(prefix, StringComparison.Ordinal) || !key.EndsWith(']'))
        {
            return false;
        }

        var value = key[prefix.Length..^1];
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out index);
    }

    private static Dictionary<string, string> ParseFields(string value)
    {
        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        var parts = value.Split(';');

        for (var i = 0; i < parts.Length; i++)
        {
            var part = parts[i].Trim();
            var separator = part.IndexOf('=');
            if (separator <= 0)
            {
                continue;
            }

            fields[part[..separator]] = part[(separator + 1)..];
        }

        return fields;
    }

    private static IReadOnlyList<string> ParseActiveSources(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return [];
        }

        return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }
}
