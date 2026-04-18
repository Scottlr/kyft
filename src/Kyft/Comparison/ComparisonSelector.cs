namespace Kyft;

/// <summary>
/// Describes a selection used by a window comparison plan.
/// </summary>
/// <remarks>
/// Prefer descriptor selectors such as <see cref="ForSource" /> and
/// <see cref="ForWindowName" /> for persistent plans. Runtime-only selectors
/// are useful locally, but cannot be exported as plan data.
/// </remarks>
public readonly record struct ComparisonSelector
{
    private readonly Func<WindowRecord, bool>? predicate;

    private ComparisonSelector(
        string name,
        string description,
        bool isSerializable,
        Func<WindowRecord, bool>? predicate)
    {
        Name = name;
        Description = description;
        IsSerializable = isSerializable;
        this.predicate = predicate;
    }

    /// <summary>
    /// Gets the selector name used in output and diagnostics.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets a readable description of the selector.
    /// </summary>
    public string Description { get; }

    /// <summary>
    /// Gets whether the selector can be exported as plan data.
    /// </summary>
    public bool IsSerializable { get; }

    /// <summary>
    /// Creates a serializable selector descriptor.
    /// </summary>
    /// <param name="name">The selector name.</param>
    /// <param name="description">A readable selector description.</param>
    /// <returns>A serializable comparison selector.</returns>
    public static ComparisonSelector Serializable(string name, string description)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(description);

        return new ComparisonSelector(name, description, isSerializable: true, predicate: null);
    }

    /// <summary>
    /// Creates a selector for a configured window name.
    /// </summary>
    /// <param name="windowName">The configured window name.</param>
    /// <returns>A serializable window-name selector.</returns>
    public static ComparisonSelector ForWindowName(string windowName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(windowName);

        return new ComparisonSelector(
            $"window:{windowName}",
            $"window name = {windowName}",
            isSerializable: true,
            window => string.Equals(window.WindowName, windowName, StringComparison.Ordinal));
    }

    /// <summary>
    /// Creates a selector for a recorded window key.
    /// </summary>
    /// <param name="key">The recorded window key.</param>
    /// <returns>A serializable key selector.</returns>
    public static ComparisonSelector ForKey(object key)
    {
        ArgumentNullException.ThrowIfNull(key);

        return new ComparisonSelector(
            $"key:{key}",
            $"key = {key}",
            isSerializable: true,
            window => EqualityComparer<object>.Default.Equals(window.Key, key));
    }

    /// <summary>
    /// Creates a selector for a source identity.
    /// </summary>
    /// <param name="source">The source identity.</param>
    /// <returns>A serializable source selector.</returns>
    public static ComparisonSelector ForSource(object source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new ComparisonSelector(
            $"source:{source}",
            $"source = {source}",
            isSerializable: true,
            window => EqualityComparer<object?>.Default.Equals(window.Source, source));
    }

    /// <summary>
    /// Creates a selector for a partition identity.
    /// </summary>
    /// <param name="partition">The partition identity.</param>
    /// <returns>A serializable partition selector.</returns>
    public static ComparisonSelector ForPartition(object partition)
    {
        ArgumentNullException.ThrowIfNull(partition);

        return new ComparisonSelector(
            $"partition:{partition}",
            $"partition = {partition}",
            isSerializable: true,
            window => EqualityComparer<object?>.Default.Equals(window.Partition, partition));
    }

    /// <summary>
    /// Creates a selector for windows whose start position is inside a half-open processing-position range.
    /// </summary>
    /// <param name="startInclusive">The inclusive start position.</param>
    /// <param name="endExclusive">The optional exclusive end position.</param>
    /// <returns>A serializable processing-position range selector.</returns>
    public static ComparisonSelector ForPositionRange(long startInclusive, long? endExclusive = null)
    {
        if (endExclusive.HasValue && endExclusive.Value < startInclusive)
        {
            throw new ArgumentException("Position range end cannot be earlier than the start.", nameof(endExclusive));
        }

        return new ComparisonSelector(
            $"position:{startInclusive}..{endExclusive?.ToString() ?? "*"}",
            $"start position in [{startInclusive}, {endExclusive?.ToString() ?? "*"})",
            isSerializable: true,
            window => window.StartPosition >= startInclusive
                && (!endExclusive.HasValue || window.StartPosition < endExclusive.Value));
    }

    /// <summary>
    /// Creates a selector for windows whose start timestamp is inside a half-open timestamp range.
    /// </summary>
    /// <param name="startInclusive">The inclusive start timestamp.</param>
    /// <param name="endExclusive">The optional exclusive end timestamp.</param>
    /// <returns>A serializable timestamp range selector.</returns>
    public static ComparisonSelector ForTimeRange(DateTimeOffset startInclusive, DateTimeOffset? endExclusive = null)
    {
        if (endExclusive.HasValue && endExclusive.Value < startInclusive)
        {
            throw new ArgumentException("Time range end cannot be earlier than the start.", nameof(endExclusive));
        }

        return new ComparisonSelector(
            $"time:{startInclusive:O}..{endExclusive?.ToString("O") ?? "*"}",
            $"start time in [{startInclusive:O}, {endExclusive?.ToString("O") ?? "*"})",
            isSerializable: true,
            window => window.StartTime.HasValue
                && window.StartTime.Value >= startInclusive
                && (!endExclusive.HasValue || window.StartTime.Value < endExclusive.Value));
    }

    /// <summary>
    /// Creates a runtime-only selector descriptor.
    /// </summary>
    /// <param name="name">The selector name.</param>
    /// <param name="description">A readable selector description.</param>
    /// <returns>A runtime-only comparison selector.</returns>
    public static ComparisonSelector RuntimeOnly(string name, string description)
    {
        return RuntimeOnly(name, description, static _ => true);
    }

    /// <summary>
    /// Creates a runtime-only selector backed by a delegate.
    /// </summary>
    /// <param name="name">The selector name.</param>
    /// <param name="description">A readable selector description.</param>
    /// <param name="predicate">The runtime predicate.</param>
    /// <returns>A runtime-only comparison selector.</returns>
    public static ComparisonSelector RuntimeOnly(
        string name,
        string description,
        Func<WindowRecord, bool> predicate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(description);
        ArgumentNullException.ThrowIfNull(predicate);

        return new ComparisonSelector(name, description, isSerializable: false, predicate);
    }

    /// <summary>
    /// Creates a selector that requires both selectors to match.
    /// </summary>
    /// <param name="other">The selector to combine with this selector.</param>
    /// <returns>A combined selector.</returns>
    public ComparisonSelector And(ComparisonSelector other)
    {
        var current = this;

        return new ComparisonSelector(
            $"{Name}&{other.Name}",
            $"({Description}) and ({other.Description})",
            IsSerializable && other.IsSerializable,
            window => current.Matches(window) && other.Matches(window));
    }

    /// <summary>
    /// Creates a selector that allows either selector to match.
    /// </summary>
    /// <param name="other">The selector to combine with this selector.</param>
    /// <returns>A combined selector.</returns>
    public ComparisonSelector Or(ComparisonSelector other)
    {
        var current = this;

        return new ComparisonSelector(
            $"{Name}|{other.Name}",
            $"({Description}) or ({other.Description})",
            IsSerializable && other.IsSerializable,
            window => current.Matches(window) || other.Matches(window));
    }

    /// <summary>
    /// Determines whether the selector matches a recorded window.
    /// </summary>
    /// <param name="window">The recorded window to test.</param>
    /// <returns><see langword="true" /> when the selector matches.</returns>
    public bool Matches(WindowRecord window)
    {
        ArgumentNullException.ThrowIfNull(window);

        return this.predicate?.Invoke(window) ?? true;
    }
}
