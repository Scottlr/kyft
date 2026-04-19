namespace Kyft.Internal.Keys;

internal sealed class RuntimeStateKeyComparer : IEqualityComparer<RuntimeStateKey>
{
    private readonly IEqualityComparer<object> keyComparer;

    public RuntimeStateKeyComparer(IEqualityComparer<object> keyComparer)
    {
        this.keyComparer = keyComparer;
    }

    public bool Equals(RuntimeStateKey x, RuntimeStateKey y)
    {
        return this.keyComparer.Equals(x.Key, y.Key)
            && EqualityComparer<object?>.Default.Equals(x.Source, y.Source)
            && EqualityComparer<object?>.Default.Equals(x.Partition, y.Partition);
    }

    public int GetHashCode(RuntimeStateKey obj)
    {
        return HashCode.Combine(
            this.keyComparer.GetHashCode(obj.Key),
            obj.Source,
            obj.Partition);
    }
}
