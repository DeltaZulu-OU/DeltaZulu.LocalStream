namespace DeltaZulu.LocalStream.Query.Time;

/// <summary>Persistable watermark progress for one source partition.</summary>
public sealed record PartitionWatermarkState(
    int Partition,
    DateTimeOffset MaxEventTimeUtc,
    DateTimeOffset LastObservedUtc);

/// <summary>Persistable snapshot used to restore monotonic watermark progress.</summary>
public sealed record WatermarkState(
    DateTimeOffset? WatermarkUtc,
    IReadOnlyList<PartitionWatermarkState> Partitions);
