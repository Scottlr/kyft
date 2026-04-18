namespace Kyft;

/// <summary>
/// Represents a prepared comparison after temporal segment alignment.
/// </summary>
/// <param name="Prepared">The prepared comparison input.</param>
/// <param name="Segments">The deterministic aligned segments.</param>
public sealed record AlignedComparison(
    PreparedComparison Prepared,
    IReadOnlyList<AlignedSegment> Segments);
