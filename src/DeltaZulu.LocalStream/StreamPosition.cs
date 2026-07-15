namespace DeltaZulu.LocalStream;

/// <summary>
/// Immutable stream coordinates identifying a single record in a topic partition.
/// </summary>
public sealed record StreamPosition(
    string Topic,
    int Partition,
    long Offset);
