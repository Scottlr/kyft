namespace Kyft.Internal.Definitions;

internal sealed class PipelineBuilderOptions<TEvent>
{
    public bool RecordWindows { get; set; }

    public Func<TEvent, DateTimeOffset>? EventTimeSelector { get; set; }
}
