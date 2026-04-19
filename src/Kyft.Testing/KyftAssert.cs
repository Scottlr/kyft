namespace Kyft.Testing;

/// <summary>
/// Provides framework-neutral assertions for Kyft comparison artifacts.
/// </summary>
public static class KyftAssert
{
    /// <summary>
    /// Asserts that a comparison result is valid.
    /// </summary>
    /// <param name="result">The result to inspect.</param>
    /// <exception cref="KyftAssertionException">Thrown when the result contains error diagnostics.</exception>
    public static void IsValid(ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        if (result.IsValid)
        {
            return;
        }

        throw new KyftAssertionException("Expected a valid Kyft result, but error diagnostics were present.");
    }

    /// <summary>
    /// Asserts that a comparison result contains no diagnostics.
    /// </summary>
    /// <param name="result">The result to inspect.</param>
    /// <exception cref="KyftAssertionException">Thrown when any diagnostic is present.</exception>
    public static void HasNoDiagnostics(ComparisonResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        if (result.Diagnostics.Count == 0)
        {
            return;
        }

        throw new KyftAssertionException("Expected no Kyft diagnostics, but found " + result.Diagnostics.Count.ToString(System.Globalization.CultureInfo.InvariantCulture) + ".");
    }

    /// <summary>
    /// Asserts that a comparison result contains a diagnostic code.
    /// </summary>
    /// <param name="result">The result to inspect.</param>
    /// <param name="code">The diagnostic code to find.</param>
    /// <returns>The matching diagnostic.</returns>
    /// <exception cref="KyftAssertionException">Thrown when the diagnostic code is missing.</exception>
    public static ComparisonPlanDiagnostic HasDiagnostic(
        ComparisonResult result,
        ComparisonPlanValidationCode code)
    {
        ArgumentNullException.ThrowIfNull(result);

        for (var i = 0; i < result.Diagnostics.Count; i++)
        {
            if (result.Diagnostics[i].Code == code)
            {
                return result.Diagnostics[i];
            }
        }

        throw new KyftAssertionException("Expected Kyft diagnostic " + code + ".");
    }

    /// <summary>
    /// Asserts that a named row collection contains an expected number of rows.
    /// </summary>
    /// <param name="result">The result to inspect.</param>
    /// <param name="rowType">The row family, such as overlap, residual, missing, or coverage.</param>
    /// <param name="expectedCount">The expected row count.</param>
    /// <exception cref="KyftAssertionException">Thrown when the row count differs.</exception>
    public static void HasRowCount(ComparisonResult result, string rowType, int expectedCount)
    {
        ArgumentNullException.ThrowIfNull(result);
        ArgumentException.ThrowIfNullOrWhiteSpace(rowType);
        ArgumentOutOfRangeException.ThrowIfNegative(expectedCount);

        var actualCount = GetRowCount(result, rowType);
        if (actualCount == expectedCount)
        {
            return;
        }

        throw new KyftAssertionException(
            "Expected "
            + expectedCount.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + " "
            + rowType
            + " rows, but found "
            + actualCount.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + ".");
    }

    private static int GetRowCount(ComparisonResult result, string rowType)
    {
        return rowType switch
        {
            "overlap" => result.OverlapRows.Count,
            "residual" => result.ResidualRows.Count,
            "missing" => result.MissingRows.Count,
            "coverage" => result.CoverageRows.Count,
            "gap" => result.GapRows.Count,
            "symmetricDifference" => result.SymmetricDifferenceRows.Count,
            "containment" => result.ContainmentRows.Count,
            "leadLag" => result.LeadLagRows.Count,
            "asOf" => result.AsOfRows.Count,
            _ => throw new ArgumentException("Unknown Kyft row type: " + rowType, nameof(rowType))
        };
    }
}
