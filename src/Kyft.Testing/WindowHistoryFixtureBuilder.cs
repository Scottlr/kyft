using System.Collections;
using System.Reflection;

namespace Kyft.Testing;

/// <summary>
/// Builds small window histories for comparison tests without running a full event pipeline.
/// </summary>
/// <remarks>
/// This helper is intended for concise contract and comparator tests. Prefer
/// normal Kyft pipeline ingestion when the test is about runtime window
/// emission behavior.
/// </remarks>
public sealed class WindowHistoryFixtureBuilder
{
    private readonly List<ClosedWindow> closedWindows = [];
    private readonly List<OpenWindow> openWindows = [];

    /// <summary>
    /// Adds a closed window to the fixture history.
    /// </summary>
    /// <param name="windowName">The configured window name.</param>
    /// <param name="key">The window key.</param>
    /// <param name="startPosition">The inclusive start processing position.</param>
    /// <param name="endPosition">The exclusive end processing position.</param>
    /// <param name="source">The optional source identity.</param>
    /// <param name="partition">The optional partition identity.</param>
    /// <param name="segments">The optional analytical segment values.</param>
    /// <param name="tags">The optional descriptive tags.</param>
    /// <returns>This builder.</returns>
    public WindowHistoryFixtureBuilder AddClosedWindow(
        string windowName,
        object key,
        long startPosition,
        long endPosition,
        object? source = null,
        object? partition = null,
        IReadOnlyList<WindowSegment>? segments = null,
        IReadOnlyList<WindowTag>? tags = null)
    {
        this.closedWindows.Add(new ClosedWindow(
            windowName,
            key,
            startPosition,
            endPosition,
            source,
            partition,
            Segments: segments,
            Tags: tags));
        return this;
    }

    /// <summary>
    /// Adds a closed window to the fixture history using a window builder.
    /// </summary>
    /// <param name="windowName">The configured window name.</param>
    /// <param name="key">The window key.</param>
    /// <param name="startPosition">The inclusive start processing position.</param>
    /// <param name="endPosition">The exclusive end processing position.</param>
    /// <param name="configure">Configures source, partition, segments, and tags.</param>
    /// <returns>This builder.</returns>
    public WindowHistoryFixtureBuilder AddClosedWindow(
        string windowName,
        object key,
        long startPosition,
        long endPosition,
        Action<WindowHistoryFixtureWindowBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new WindowHistoryFixtureWindowBuilder();
        configure(builder);

        return AddClosedWindow(
            windowName,
            key,
            startPosition,
            endPosition,
            builder.SourceValue,
            builder.PartitionValue,
            builder.Segments,
            builder.Tags);
    }

    /// <summary>
    /// Adds an open window to the fixture history.
    /// </summary>
    /// <param name="windowName">The configured window name.</param>
    /// <param name="key">The window key.</param>
    /// <param name="startPosition">The inclusive start processing position.</param>
    /// <param name="source">The optional source identity.</param>
    /// <param name="partition">The optional partition identity.</param>
    /// <param name="segments">The optional analytical segment values.</param>
    /// <param name="tags">The optional descriptive tags.</param>
    /// <returns>This builder.</returns>
    public WindowHistoryFixtureBuilder AddOpenWindow(
        string windowName,
        object key,
        long startPosition,
        object? source = null,
        object? partition = null,
        IReadOnlyList<WindowSegment>? segments = null,
        IReadOnlyList<WindowTag>? tags = null)
    {
        this.openWindows.Add(new OpenWindow(
            windowName,
            key,
            startPosition,
            source,
            partition,
            Segments: segments,
            Tags: tags));
        return this;
    }

    /// <summary>
    /// Adds an open window to the fixture history using a window builder.
    /// </summary>
    /// <param name="windowName">The configured window name.</param>
    /// <param name="key">The window key.</param>
    /// <param name="startPosition">The inclusive start processing position.</param>
    /// <param name="configure">Configures source, partition, segments, and tags.</param>
    /// <returns>This builder.</returns>
    public WindowHistoryFixtureBuilder AddOpenWindow(
        string windowName,
        object key,
        long startPosition,
        Action<WindowHistoryFixtureWindowBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new WindowHistoryFixtureWindowBuilder();
        configure(builder);

        return AddOpenWindow(
            windowName,
            key,
            startPosition,
            builder.SourceValue,
            builder.PartitionValue,
            builder.Segments,
            builder.Tags);
    }

    /// <summary>
    /// Builds a Kyft window history containing the configured windows.
    /// </summary>
    /// <returns>A window history fixture.</returns>
    public WindowIntervalHistory Build()
    {
        var constructor = typeof(WindowIntervalHistory).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [typeof(bool)],
            modifiers: null)
            ?? throw new InvalidOperationException("Kyft history constructor was not found.");
        var history = (WindowIntervalHistory)constructor.Invoke([true]);
        var field = typeof(WindowIntervalHistory).GetField(
            "closedIntervals",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Kyft closed-window storage was not found.");
        var closed = (List<ClosedWindow>)field.GetValue(history)!;

        for (var i = 0; i < this.closedWindows.Count; i++)
        {
            closed.Add(this.closedWindows[i]);
        }

        AddOpenWindows(history);

        return history;
    }

    private void AddOpenWindows(WindowIntervalHistory history)
    {
        if (this.openWindows.Count == 0)
        {
            return;
        }

        var keyType = typeof(WindowIntervalHistory).Assembly.GetType("Kyft.Internal.Recording.WindowRecordingKey")
            ?? throw new InvalidOperationException("Kyft window recording key type was not found.");
        var field = typeof(WindowIntervalHistory).GetField(
            "openIntervals",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Kyft open-window storage was not found.");
        var open = (IDictionary)field.GetValue(history)!;

        for (var i = 0; i < this.openWindows.Count; i++)
        {
            var window = this.openWindows[i];
            var key = Activator.CreateInstance(
                keyType,
                window.WindowName,
                window.Key,
                window.Source,
                window.Partition,
                StableSegments(window.Segments))
                ?? throw new InvalidOperationException("Kyft window recording key could not be created.");
            open.Add(key, window);
        }
    }

    private static string StableSegments(IReadOnlyList<WindowSegment> segments)
    {
        if (segments.Count == 0)
        {
            return string.Empty;
        }

        var builder = new System.Text.StringBuilder();
        for (var i = 0; i < segments.Count; i++)
        {
            var segment = segments[i];
            builder
                .Append(segment.ParentName ?? string.Empty)
                .Append('/')
                .Append(segment.Name)
                .Append('=')
                .Append(segment.Value)
                .Append(';');
        }

        return builder.ToString();
    }
}
