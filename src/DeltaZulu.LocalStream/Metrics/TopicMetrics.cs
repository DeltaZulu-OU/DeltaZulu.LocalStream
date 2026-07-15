namespace DeltaZulu.LocalStream;

/// <summary>Point-in-time metrics for one topic partition.</summary>
public sealed record PartitionMetrics(
    int Partition,
    long EarliestRetainedOffset,
    long NextOffset,
    long SizeBytes,
    int SegmentCount);

/// <summary>Point-in-time metrics for one topic across all partitions.</summary>
public sealed record TopicMetrics(
    string Topic,
    long RecordsTotal,
    long SizeBytes,
    int SegmentCount,
    IReadOnlyList<PartitionMetrics> Partitions);
