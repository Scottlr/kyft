namespace Spanfold;

/// <summary>
/// Summarizes target coverage within one comparison scope.
/// </summary>
/// <param name="WindowName">The configured window name.</param>
/// <param name="Key">The logical window key.</param>
/// <param name="Partition">The optional partition identity.</param>
/// <param name="TargetMagnitude">The denominator magnitude.</param>
/// <param name="CoveredMagnitude">The covered numerator magnitude.</param>
/// <param name="CoverageRatio">The covered numerator divided by the denominator.</param>
public sealed record CoverageSummary(
    string WindowName,
    object Key,
    object? Partition,
    double TargetMagnitude,
    double CoveredMagnitude,
    double CoverageRatio);
