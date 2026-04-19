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

    /// <summary>
    /// Adds a closed window to the fixture history.
    /// </summary>
    /// <param name="windowName">The configured window name.</param>
    /// <param name="key">The window key.</param>
    /// <param name="startPosition">The inclusive start processing position.</param>
    /// <param name="endPosition">The exclusive end processing position.</param>
    /// <param name="source">The optional source identity.</param>
    /// <param name="partition">The optional partition identity.</param>
    /// <returns>This builder.</returns>
    public WindowHistoryFixtureBuilder AddClosedWindow(
        string windowName,
        object key,
        long startPosition,
        long endPosition,
        object? source = null,
        object? partition = null)
    {
        this.closedWindows.Add(new ClosedWindow(windowName, key, startPosition, endPosition, source, partition));
        return this;
    }

    /// <summary>
    /// Builds a Kyft window history containing the configured closed windows.
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

        return history;
    }
}
