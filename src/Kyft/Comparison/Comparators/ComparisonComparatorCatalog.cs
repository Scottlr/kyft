namespace Kyft;

/// <summary>
/// Describes comparator declarations understood by core Kyft.
/// </summary>
/// <remarks>
/// The catalog is intended for tooling and fixture validation.
/// Runtime execution is still driven by declarations in the comparison plan.
/// Extension packages can expose additional declarations with
/// <see cref="ComparisonExtensionBuilder" />.
/// </remarks>
public static class ComparisonComparatorCatalog
{
    private static readonly string[] BuiltIns =
    [
        "overlap",
        "residual",
        "missing",
        "coverage",
        "gap",
        "symmetric-difference",
        "containment"
    ];

    /// <summary>
    /// Gets exact built-in comparator declarations.
    /// </summary>
    public static IReadOnlyList<string> BuiltInDeclarations => BuiltIns;

    /// <summary>
    /// Returns true when the declaration is an exact built-in comparator name.
    /// </summary>
    /// <param name="declaration">The comparator declaration.</param>
    /// <returns>True when the declaration is an exact built-in comparator name.</returns>
    public static bool IsBuiltInDeclaration(string declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);

        for (var i = 0; i < BuiltIns.Length; i++)
        {
            if (string.Equals(BuiltIns[i], declaration, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns true when core Kyft can execute the comparator declaration.
    /// </summary>
    /// <param name="declaration">The comparator declaration.</param>
    /// <returns>True when core Kyft can execute the declaration.</returns>
    public static bool IsKnownDeclaration(string declaration)
    {
        ArgumentNullException.ThrowIfNull(declaration);

        return IsBuiltInDeclaration(declaration)
            || IsLeadLagDeclaration(declaration)
            || IsAsOfDeclaration(declaration);
    }

    private static bool IsLeadLagDeclaration(string declaration)
    {
        var parts = declaration.Split(':');
        return parts.Length == 4
            && string.Equals(parts[0], "lead-lag", StringComparison.Ordinal)
            && Enum.TryParse<LeadLagTransition>(parts[1], ignoreCase: false, out _)
            && Enum.TryParse<TemporalAxis>(parts[2], ignoreCase: false, out var axis)
            && axis != TemporalAxis.Unknown
            && long.TryParse(parts[3], out var tolerance)
            && tolerance >= 0;
    }

    private static bool IsAsOfDeclaration(string declaration)
    {
        var parts = declaration.Split(':');
        return parts.Length == 4
            && string.Equals(parts[0], "asof", StringComparison.Ordinal)
            && Enum.TryParse<AsOfDirection>(parts[1], ignoreCase: false, out _)
            && Enum.TryParse<TemporalAxis>(parts[2], ignoreCase: false, out var axis)
            && axis != TemporalAxis.Unknown
            && long.TryParse(parts[3], out var tolerance)
            && tolerance >= 0;
    }
}
