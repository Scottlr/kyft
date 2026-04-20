namespace Kyft.Internal.Runtime;

internal sealed record ActiveWindowState(
    IReadOnlyList<WindowSegment> Segments,
    IReadOnlyList<WindowTag> Tags);
