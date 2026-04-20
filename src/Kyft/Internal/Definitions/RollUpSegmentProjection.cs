namespace Kyft.Internal.Definitions;

internal sealed class RollUpSegmentProjection
{
    public static RollUpSegmentProjection PreserveAll { get; } = new(
        [],
        [],
        new Dictionary<string, string>(StringComparer.Ordinal),
        new Dictionary<string, Func<object?, object?>>(StringComparer.Ordinal));

    private readonly HashSet<string> preservedNames;
    private readonly HashSet<string> droppedNames;
    private readonly Dictionary<string, string> renamedNames;
    private readonly Dictionary<string, Func<object?, object?>> valueTransforms;

    public RollUpSegmentProjection(
        IEnumerable<string> preservedNames,
        IEnumerable<string> droppedNames,
        IReadOnlyDictionary<string, string> renamedNames,
        IReadOnlyDictionary<string, Func<object?, object?>> valueTransforms)
    {
        this.preservedNames = new HashSet<string>(
            preservedNames,
            StringComparer.Ordinal);
        this.droppedNames = new HashSet<string>(
            droppedNames,
            StringComparer.Ordinal);
        this.renamedNames = new Dictionary<string, string>(
            renamedNames,
            StringComparer.Ordinal);
        this.valueTransforms = new Dictionary<string, Func<object?, object?>>(
            valueTransforms,
            StringComparer.Ordinal);
    }

    public IReadOnlyList<WindowSegment> Project(IReadOnlyList<WindowSegment> segments)
    {
        if (segments.Count == 0 || !HasProjectionRules)
        {
            return segments;
        }

        var selectedSegments = new List<WindowSegment>(segments.Count);
        var selectedOriginalNames = new HashSet<string>(StringComparer.Ordinal);
        var selectedProjectedNames = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < segments.Count; i++)
        {
            var segment = segments[i];
            if (!ShouldKeep(segment.Name))
            {
                continue;
            }

            var projectedName = ProjectName(segment.Name);
            if (!selectedProjectedNames.Add(projectedName))
            {
                throw new InvalidOperationException(
                    $"Roll-up segment projection produced duplicate segment '{projectedName}'.");
            }

            selectedSegments.Add(new WindowSegment(
                projectedName,
                ProjectValue(segment.Name, segment.Value),
                segment.ParentName));
            selectedOriginalNames.Add(segment.Name);
        }

        if (selectedSegments.Count == 0)
        {
            return [];
        }

        for (var i = 0; i < selectedSegments.Count; i++)
        {
            var segment = selectedSegments[i];
            if (segment.ParentName is null)
            {
                continue;
            }

            selectedSegments[i] = selectedOriginalNames.Contains(segment.ParentName)
                ? segment with { ParentName = ProjectName(segment.ParentName) }
                : segment with { ParentName = null };
        }

        return selectedSegments.ToArray();
    }

    private bool HasProjectionRules =>
        this.preservedNames.Count > 0
        || this.droppedNames.Count > 0
        || this.renamedNames.Count > 0
        || this.valueTransforms.Count > 0;

    private string ProjectName(string name)
    {
        return this.renamedNames.TryGetValue(name, out var projectedName)
            ? projectedName
            : name;
    }

    private object? ProjectValue(string name, object? value)
    {
        return this.valueTransforms.TryGetValue(name, out var transform)
            ? transform(value)
            : value;
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
