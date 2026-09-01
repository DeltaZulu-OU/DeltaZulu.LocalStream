namespace DeltaZulu.LocalStream.Query.Operators;

/// <summary>Already-bound input required by the tumbling aggregate runtime.</summary>
public sealed record TumblingAggregateInput
{
    public TumblingAggregateInput(
        int partition,
        string logicalKey,
        DateTimeOffset eventTimeUtc,
        ReadOnlyMemory<byte> canonicalDistinctValue)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalKey);
        Partition = partition;
        LogicalKey = logicalKey;
        EventTimeUtc = eventTimeUtc.ToUniversalTime();
        CanonicalDistinctValue = canonicalDistinctValue.ToArray();
    }

    public int Partition { get; }
    public string LogicalKey { get; }
    public DateTimeOffset EventTimeUtc { get; }
    public ReadOnlyMemory<byte> CanonicalDistinctValue { get; }
}
