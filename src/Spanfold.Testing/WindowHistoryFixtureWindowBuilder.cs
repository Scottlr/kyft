namespace Spanfold.Testing;

/// <summary>
/// Builds one recorded window for a <see cref="WindowHistoryFixtureBuilder" />.
/// </summary>
public sealed class WindowHistoryFixtureWindowBuilder
{
    private readonly List<WindowSegment> segments = [];
    private readonly List<WindowTag> tags = [];

    internal WindowHistoryFixtureWindowBuilder()
    {
    }

    /// <summary>
    /// Gets the configured source identity.
    /// </summary>
    public object? SourceValue { get; private set; }

    /// <summary>
    /// Gets the configured partition identity.
    /// </summary>
    public object? PartitionValue { get; private set; }

    internal IReadOnlyList<WindowSegment> Segments => this.segments;

    internal IReadOnlyList<WindowTag> Tags => this.tags;

    /// <summary>
    /// Sets the source identity for the fixture window.
    /// </summary>
    /// <param name="source">The source identity.</param>
    /// <returns>The current builder.</returns>
    public WindowHistoryFixtureWindowBuilder Source(object source)
    {
        ArgumentNullException.ThrowIfNull(source);

        SourceValue = source;
        return this;
    }

    /// <summary>
    /// Sets the partition identity for the fixture window.
    /// </summary>
    /// <param name="partition">The partition identity.</param>
    /// <returns>The current builder.</returns>
    public WindowHistoryFixtureWindowBuilder Partition(object partition)
    {
        ArgumentNullException.ThrowIfNull(partition);

        PartitionValue = partition;
        return this;
    }

    /// <summary>
    /// Adds an analytical segment value to the fixture window.
    /// </summary>
    /// <param name="name">The segment dimension name.</param>
    /// <param name="value">The segment value.</param>
    /// <param name="parentName">The optional parent segment name.</param>
    /// <returns>The current builder.</returns>
    public WindowHistoryFixtureWindowBuilder Segment(
        string name,
        object? value,
        string? parentName = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        this.segments.Add(new WindowSegment(name, value, parentName));
        return this;
    }

    /// <summary>
    /// Adds descriptive non-boundary metadata to the fixture window.
    /// </summary>
    /// <param name="name">The tag name.</param>
    /// <param name="value">The tag value.</param>
    /// <returns>The current builder.</returns>
    public WindowHistoryFixtureWindowBuilder Tag(string name, object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        this.tags.Add(new WindowTag(name, value));
        return this;
    }
}
