namespace Kyft.Internal.Definitions;

internal sealed class RollUpSegmentProjection
{
    public static RollUpSegmentProjection PreserveAll { get; } = new([], []);

    private readonly HashSet<string> preservedNames;
    private readonly HashSet<string> droppedNames;

    public RollUpSegmentProjection(
        IEnumerable<string> preservedNames,
        IEnumerable<string> droppedNames)
    {
        this.preservedNames = new HashSet<string>(
            preservedNames,
            StringComparer.Ordinal);
        this.droppedNames = new HashSet<string>(
            droppedNames,
            StringComparer.Ordinal);
    }

    public IReadOnlyList<WindowSegment> Project(IReadOnlyList<WindowSegment> segments)
    {
        if (segments.Count == 0 || (this.preservedNames.Count == 0 && this.droppedNames.Count == 0))
        {
            return segments;
        }

        var selectedSegments = new List<WindowSegment>(segments.Count);
        var selectedNames = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < segments.Count; i++)
        {
            var segment = segments[i];
            if (!ShouldKeep(segment.Name))
            {
                continue;
            }

            selectedSegments.Add(segment);
            selectedNames.Add(segment.Name);
        }

        if (selectedSegments.Count == 0)
        {
            return [];
        }

        for (var i = 0; i < selectedSegments.Count; i++)
        {
            var segment = selectedSegments[i];
            if (segment.ParentName is null || selectedNames.Contains(segment.ParentName))
            {
                continue;
            }

            selectedSegments[i] = segment with { ParentName = null };
        }

        return selectedSegments.ToArray();
    }

    private bool ShouldKeep(string name)
    {
        if (this.droppedNames.Contains(name))
        {
            return false;
        }

        return this.preservedNames.Count == 0
            || this.preservedNames.Contains(name);
    }
}
