namespace Kyft;

/// <summary>
/// Describes non-boundary metadata attached to a recorded window.
/// </summary>
/// <remarks>
/// Tags describe a window but do not split active windows when their values
/// change. Use segments for values such as phase or period that should create
/// analytical boundaries.
/// </remarks>
/// <param name="Name">The tag name.</param>
/// <param name="Value">The tag value.</param>
public sealed record WindowTag(
    string Name,
    object? Value);
