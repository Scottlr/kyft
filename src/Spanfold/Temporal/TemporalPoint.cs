namespace Spanfold;

/// <summary>
/// Represents a single point on a temporal axis used by Spanfold analysis.
/// </summary>
/// <remarks>
/// A temporal point is either a processing position or a timestamp. Points on
/// different axes are not comparable unless a later analysis stage explicitly
/// maps one axis to another.
/// </remarks>
public readonly record struct TemporalPoint : IComparable<TemporalPoint>
{
    private readonly long position;
    private readonly DateTimeOffset timestamp;

    private TemporalPoint(
        TemporalAxis axis,
        long position,
        DateTimeOffset timestamp,
        string? clock)
    {
        Axis = axis;
        this.position = position;
        this.timestamp = timestamp;
        Clock = clock;
    }

    /// <summary>
    /// Gets the temporal axis for this point.
    /// </summary>
    public TemporalAxis Axis { get; }

    /// <summary>
    /// Gets the optional clock identity for timestamp points.
    /// </summary>
    /// <remarks>
    /// Clock identity is only meaningful when <see cref="Axis" /> is
    /// <see cref="TemporalAxis.Timestamp" />. Timestamp points with different
    /// clock identities are not comparable by default.
    /// </remarks>
    public string? Clock { get; }

    /// <summary>
    /// Gets the processing position value.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this point is not a processing-position point.
    /// </exception>
    public long Position => Axis == TemporalAxis.ProcessingPosition
        ? this.position
        : throw new InvalidOperationException("Only processing-position points expose a position value.");

    /// <summary>
    /// Gets the timestamp value.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this point is not a timestamp point.
    /// </exception>
    public DateTimeOffset Timestamp => Axis == TemporalAxis.Timestamp
        ? this.timestamp
        : throw new InvalidOperationException("Only timestamp points expose a timestamp value.");

    /// <summary>
    /// Creates a point ordered by pipeline processing position.
    /// </summary>
    /// <param name="position">The processing position assigned during ingestion.</param>
    /// <returns>A temporal point on the processing-position axis.</returns>
    public static TemporalPoint ForPosition(long position)
    {
        return new TemporalPoint(
            TemporalAxis.ProcessingPosition,
            position,
            timestamp: default,
            clock: null);
    }

    /// <summary>
    /// Creates a point ordered by event timestamp.
    /// </summary>
    /// <param name="timestamp">The event timestamp.</param>
    /// <param name="clock">Optional identity for the timestamp clock.</param>
    /// <returns>A temporal point on the timestamp axis.</returns>
    public static TemporalPoint ForTimestamp(DateTimeOffset timestamp, string? clock = null)
    {
        return new TemporalPoint(
            TemporalAxis.Timestamp,
            position: default,
            timestamp,
            clock);
    }

    /// <summary>
    /// Compares this point with another point on the same temporal axis.
    /// </summary>
    /// <param name="other">The other point to compare.</param>
    /// <returns>
    /// A negative value when this point is earlier, zero when both points are
    /// equal, or a positive value when this point is later.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the points are on different axes or incompatible timestamp
    /// clocks.
    /// </exception>
    public int CompareTo(TemporalPoint other)
    {
        EnsureComparable(other);

        return Axis switch
        {
            TemporalAxis.ProcessingPosition => this.position.CompareTo(other.position),
            TemporalAxis.Timestamp => this.timestamp.CompareTo(other.timestamp),
            _ => throw new InvalidOperationException("Unknown temporal points are not comparable.")
        };
    }

    /// <summary>
    /// Returns whether this point is earlier than another comparable point.
    /// </summary>
    /// <param name="other">The other point to compare.</param>
    /// <returns><see langword="true" /> when this point is earlier.</returns>
    public bool IsBefore(TemporalPoint other)
    {
        return CompareTo(other) < 0;
    }

    /// <summary>
    /// Returns whether this point is later than another comparable point.
    /// </summary>
    /// <param name="other">The other point to compare.</param>
    /// <returns><see langword="true" /> when this point is later.</returns>
    public bool IsAfter(TemporalPoint other)
    {
        return CompareTo(other) > 0;
    }

    /// <summary>
    /// Compares two temporal points on the same axis.
    /// </summary>
    /// <param name="left">The first point.</param>
    /// <param name="right">The second point.</param>
    /// <returns>
    /// A negative value when <paramref name="left" /> is earlier, zero when both
    /// points are equal, or a positive value when <paramref name="left" /> is
    /// later.
    /// </returns>
    public static int Compare(TemporalPoint left, TemporalPoint right)
    {
        return left.CompareTo(right);
    }

    /// <summary>
    /// Determines whether the left point is earlier than the right point.
    /// </summary>
    /// <param name="left">The left point.</param>
    /// <param name="right">The right point.</param>
    /// <returns><see langword="true" /> when the left point is earlier.</returns>
    public static bool operator <(TemporalPoint left, TemporalPoint right)
    {
        return left.CompareTo(right) < 0;
    }

    /// <summary>
    /// Determines whether the left point is later than the right point.
    /// </summary>
    /// <param name="left">The left point.</param>
    /// <param name="right">The right point.</param>
    /// <returns><see langword="true" /> when the left point is later.</returns>
    public static bool operator >(TemporalPoint left, TemporalPoint right)
    {
        return left.CompareTo(right) > 0;
    }

    /// <summary>
    /// Determines whether the left point is earlier than or equal to the right point.
    /// </summary>
    /// <param name="left">The left point.</param>
    /// <param name="right">The right point.</param>
    /// <returns><see langword="true" /> when the left point is earlier than or equal to the right point.</returns>
    public static bool operator <=(TemporalPoint left, TemporalPoint right)
    {
        return left.CompareTo(right) <= 0;
    }

    /// <summary>
    /// Determines whether the left point is later than or equal to the right point.
    /// </summary>
    /// <param name="left">The left point.</param>
    /// <param name="right">The right point.</param>
    /// <returns><see langword="true" /> when the left point is later than or equal to the right point.</returns>
    public static bool operator >=(TemporalPoint left, TemporalPoint right)
    {
        return left.CompareTo(right) >= 0;
    }

    private void EnsureComparable(TemporalPoint other)
    {
        if (Axis == TemporalAxis.Unknown || other.Axis == TemporalAxis.Unknown)
        {
            throw new InvalidOperationException("Unknown temporal points are not comparable.");
        }

        if (Axis != other.Axis)
        {
            throw new InvalidOperationException("Temporal points on different axes are not comparable.");
        }

        if (Axis == TemporalAxis.Timestamp
            && !string.Equals(Clock, other.Clock, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Timestamp points with different clock identities are not comparable.");
        }
    }
}
