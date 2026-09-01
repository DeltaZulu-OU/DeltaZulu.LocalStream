namespace DeltaZulu.LocalStream.Query.State;

/// <summary>An inclusive, ordered source range processed as one state transaction.</summary>
public sealed record SourceRange
{
    public SourceRange(string topic, int partition, long startOffset, long endOffset)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(startOffset);
        ArgumentOutOfRangeException.ThrowIfLessThan(endOffset, startOffset);
        Topic = topic;
        Partition = partition;
        StartOffset = startOffset;
        EndOffset = endOffset;
    }

    public string Topic { get; }
    public int Partition { get; }
    public long StartOffset { get; }
    public long EndOffset { get; }
}
