using Kyft;

namespace Kyft.Tests.Support;

internal sealed class VirtualComparisonClock
{
    public VirtualComparisonClock(long initialTicks = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(initialTicks);
        Ticks = initialTicks;
    }

    public long Ticks { get; private set; }

    public TemporalPoint Horizon => TemporalPoint.ForPosition(Ticks);

    public TemporalPoint AdvanceBy(long ticks)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ticks);
        Ticks += ticks;
        return Horizon;
    }

    public TemporalPoint AdvanceTo(long ticks)
    {
        if (ticks < Ticks)
        {
            throw new ArgumentOutOfRangeException(nameof(ticks), "Virtual comparison clock cannot move backwards.");
        }

        Ticks = ticks;
        return Horizon;
    }

    public TResult Check<TResult>(Func<TemporalPoint, TResult> run)
    {
        ArgumentNullException.ThrowIfNull(run);
        return run(Horizon);
    }

    public TResult InjectLateEvent<TResult>(Action inject, Func<TemporalPoint, TResult> run)
    {
        ArgumentNullException.ThrowIfNull(inject);
        ArgumentNullException.ThrowIfNull(run);

        inject();
        return run(Horizon);
    }
}
