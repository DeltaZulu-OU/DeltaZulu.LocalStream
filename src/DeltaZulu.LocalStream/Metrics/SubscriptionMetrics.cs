namespace DeltaZulu.LocalStream;

/// <summary>Per-partition subscription progress. CommittedOffset is -1 without a checkpoint.</summary>
public sealed record SubscriptionPartitionMetrics(
    int Partition,
    long CommittedOffset,
    long LagRecords,
    SubscriptionState State);

/// <summary>
/// Point-in-time lag metrics for one subscription on one topic.
/// <see cref="Required"/> reflects the configured flag: required subscriptions
/// are the evidence path and their lag should be alerted on.
/// </summary>
public sealed record SubscriptionMetrics(
    string SubscriptionId,
    string Topic,
    bool Required,
    long TotalLagRecords,
    IReadOnlyList<SubscriptionPartitionMetrics> Partitions);
