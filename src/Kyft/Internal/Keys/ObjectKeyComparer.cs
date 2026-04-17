namespace Kyft.Internal.Keys;

internal sealed class ObjectKeyComparer<TKey> : IEqualityComparer<object>
    where TKey : notnull
{
    private readonly IEqualityComparer<TKey> comparer;

    public ObjectKeyComparer(IEqualityComparer<TKey> comparer)
    {
        this.comparer = comparer;
    }

    public new bool Equals(object? x, object? y)
    {
        return x is TKey xKey && y is TKey yKey
            ? this.comparer.Equals(xKey, yKey)
            : EqualityComparer<object>.Default.Equals(x, y);
    }

    public int GetHashCode(object obj)
    {
        return obj is TKey key
            ? this.comparer.GetHashCode(key)
            : EqualityComparer<object>.Default.GetHashCode(obj);
    }
}
