namespace Spanfold;

/// <summary>
/// Describes one analytical segment value attached to a recorded window.
/// </summary>
/// <remarks>
/// Segments are boundary dimensions. When a configured segment value changes
/// while a window is active, Spanfold can close the current window and open a new
/// segment for the same key. This is distinct from runtime partition, which
/// isolates state, and from tags, which are descriptive metadata.
/// </remarks>
/// <param name="Name">The segment dimension name.</param>
/// <param name="Value">The segment value.</param>
/// <param name="ParentName">The optional parent segment name in a hierarchy.</param>
public sealed record WindowSegment(
    string Name,
    object? Value,
    string? ParentName = null);
