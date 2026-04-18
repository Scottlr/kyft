namespace Kyft;

/// <summary>
/// Represents a prepared comparison after temporal segment alignment.
/// </summary>
/// <remarks>
/// Aligned comparisons split normalized windows at every relevant boundary so
/// comparators can reason over one deterministic segment at a time.
/// </remarks>
/// <param name="Prepared">The prepared comparison input.</param>
/// <param name="Segments">The deterministic aligned segments.</param>
public sealed record AlignedComparison(
    PreparedComparison Prepared,
    IReadOnlyList<AlignedSegment> Segments);
