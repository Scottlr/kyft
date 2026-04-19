namespace Kyft.Testing;

/// <summary>
/// Provides a deterministic processing-position horizon for live comparison tests.
/// </summary>
public sealed class VirtualComparisonClock
{
    /// <summary>
    /// Creates a virtual comparison clock.
    /// </summary>
    /// <param name="initialPosition">The initial processing position.</param>
    public VirtualComparisonClock(long initialPosition = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(initialPosition);
        Position = initialPosition;
    }

    /// <summary>
    /// Gets the current virtual processing position.
    /// </summary>
    public long Position { get; private set; }

    /// <summary>
    /// Gets the current horizon as a Kyft processing-position point.
    /// </summary>
    public TemporalPoint Horizon => TemporalPoint.ForPosition(Position);

    /// <summary>
    /// Advances the clock by a non-negative position delta.
    /// </summary>
    /// <param name="positions">The number of positions to advance.</param>
    /// <returns>The updated horizon.</returns>
    public TemporalPoint AdvanceBy(long positions)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(positions);
        Position += positions;
        return Horizon;
    }

    /// <summary>
    /// Advances the clock to an absolute processing position.
    /// </summary>
    /// <param name="position">The absolute processing position.</param>
    /// <returns>The updated horizon.</returns>
    public TemporalPoint AdvanceTo(long position)
    {
        if (position < Position)
        {
            throw new ArgumentOutOfRangeException(nameof(position), "Virtual comparison clocks cannot move backwards.");
        }

        Position = position;
        return Horizon;
    }
}
