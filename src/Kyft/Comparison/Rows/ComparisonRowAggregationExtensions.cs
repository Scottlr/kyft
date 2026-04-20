namespace Kyft;

/// <summary>
/// Provides small aggregation helpers for comparison result rows.
/// </summary>
public static class ComparisonRowAggregationExtensions
{
    /// <summary>
    /// Sums event-time duration across residual rows.
    /// </summary>
    /// <param name="rows">The residual rows to aggregate.</param>
    /// <returns>The total event-time duration.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when any row is not on the timestamp axis.
    /// </exception>
    public static TimeSpan TotalTimeDuration(this IEnumerable<ResidualRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var total = TimeSpan.Zero;
        foreach (var row in rows)
        {
            total += row.Range.GetTimeDuration();
        }

        return total;
    }

    /// <summary>
    /// Sums processing-position length across residual rows.
    /// </summary>
    /// <param name="rows">The residual rows to aggregate.</param>
    /// <returns>The total processing-position length.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when any row is not on the processing-position axis.
    /// </exception>
    public static long TotalPositionLength(this IEnumerable<ResidualRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var total = 0L;
        foreach (var row in rows)
        {
            total += row.Range.GetPositionLength();
        }

        return total;
    }

    /// <summary>
    /// Sums event-time duration across missing rows.
    /// </summary>
    /// <param name="rows">The missing rows to aggregate.</param>
    /// <returns>The total event-time duration.</returns>
    public static TimeSpan TotalTimeDuration(this IEnumerable<MissingRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var total = TimeSpan.Zero;
        foreach (var row in rows)
        {
            total += row.Range.GetTimeDuration();
        }

        return total;
    }

    /// <summary>
    /// Sums processing-position length across missing rows.
    /// </summary>
    /// <param name="rows">The missing rows to aggregate.</param>
    /// <returns>The total processing-position length.</returns>
    public static long TotalPositionLength(this IEnumerable<MissingRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var total = 0L;
        foreach (var row in rows)
        {
            total += row.Range.GetPositionLength();
        }

        return total;
    }
}
