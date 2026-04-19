using System.Text.RegularExpressions;

namespace Kyft.Testing;

/// <summary>
/// Provides small snapshot helpers for Kyft JSON and markdown artifacts.
/// </summary>
public static partial class KyftSnapshot
{
    /// <summary>
    /// Normalizes line endings, trailing whitespace, and deterministic Kyft record IDs.
    /// </summary>
    /// <param name="value">The snapshot text to normalize.</param>
    /// <param name="normalizeRecordIds">Whether 64-character Kyft record IDs should be replaced with stable placeholders.</param>
    /// <returns>The normalized snapshot text.</returns>
    public static string Normalize(string value, bool normalizeRecordIds = true)
    {
        ArgumentNullException.ThrowIfNull(value);

        var normalized = value.Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace('\r', '\n')
            .TrimEnd();

        if (normalizeRecordIds)
        {
            normalized = NormalizeRecordIds(normalized);
        }

        return normalized + "\n";
    }

    /// <summary>
    /// Asserts that two snapshot strings are equal after Kyft normalization.
    /// </summary>
    /// <param name="expected">The expected snapshot.</param>
    /// <param name="actual">The actual snapshot.</param>
    /// <exception cref="KyftAssertionException">Thrown when the normalized snapshots differ.</exception>
    public static void AssertEqual(string expected, string actual)
    {
        var normalizedExpected = Normalize(expected);
        var normalizedActual = Normalize(actual);
        if (string.Equals(normalizedExpected, normalizedActual, StringComparison.Ordinal))
        {
            return;
        }

        throw new KyftAssertionException("Kyft snapshot mismatch." + Environment.NewLine + BuildDiff(normalizedExpected, normalizedActual));
    }

    private static string NormalizeRecordIds(string value)
    {
        var next = 1;
        var ids = new Dictionary<string, string>(StringComparer.Ordinal);

        return RecordIdRegex().Replace(value, match =>
        {
            if (!ids.TryGetValue(match.Value, out var replacement))
            {
                replacement = "<record-id:" + next.ToString(System.Globalization.CultureInfo.InvariantCulture) + ">";
                ids.Add(match.Value, replacement);
                next++;
            }

            return replacement;
        });
    }

    private static string BuildDiff(string expected, string actual)
    {
        var expectedLines = expected.Split('\n');
        var actualLines = actual.Split('\n');
        var max = Math.Max(expectedLines.Length, actualLines.Length);

        for (var i = 0; i < max; i++)
        {
            var expectedLine = i < expectedLines.Length ? expectedLines[i] : "<missing>";
            var actualLine = i < actualLines.Length ? actualLines[i] : "<missing>";
            if (!string.Equals(expectedLine, actualLine, StringComparison.Ordinal))
            {
                return "First difference at line " + (i + 1).ToString(System.Globalization.CultureInfo.InvariantCulture) + ".";
            }
        }

        return "Snapshots differ.";
    }

    [GeneratedRegex(@"\b[a-f0-9]{64}\b", RegexOptions.CultureInvariant)]
    private static partial Regex RecordIdRegex();
}
