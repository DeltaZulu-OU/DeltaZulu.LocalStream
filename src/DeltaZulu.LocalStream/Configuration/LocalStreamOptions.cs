namespace DeltaZulu.LocalStream;

public sealed class LocalStreamOptions
{
    /// <summary>Root directory of the local stream store.</summary>
    public required string StoragePath { get; init; }

    public IList<TopicOptions> Topics { get; } = [];

    public IList<SubscriptionOptions> Subscriptions { get; } = [];
}

public enum StartPosition
{
    /// <summary>A new subscription starts from the earliest retained record.</summary>
    Earliest,

    /// <summary>A new subscription starts after the records existing at registration.</summary>
    Latest,
}

/// <summary>
/// A named subscription registered at host startup. The start position applies
/// only when the subscription has no checkpoint yet; an existing checkpoint is
/// never reset by configuration.
/// </summary>
public sealed class SubscriptionOptions
{
    public required string Id { get; init; }

    public required string Topic { get; init; }

    /// <summary>
    /// Required subscriptions (e.g. archive) are the evidence path: operators
    /// should alert on their lag. Optional ones may sample or fall behind.
    /// </summary>
    public bool Required { get; init; }

    public StartPosition StartPosition { get; init; } = StartPosition.Earliest;
}

public sealed class TopicOptions
{
    public required string Name { get; init; }

    public int Partitions { get; init; } = 1;

    /// <summary>Segment roll threshold. A new segment starts once the active one reaches this size.</summary>
    public long MaxSegmentBytes { get; init; } = 128 * 1024 * 1024;

    /// <summary>
    /// Maximum serialized payload size per record. Larger appends are rejected
    /// with <see cref="AppendStatus.RejectedRecordTooLarge"/>. Unlimited when null.
    /// </summary>
    public long? MaxRecordBytes { get; init; }

    /// <summary>
    /// Hard disk cap for the topic across all partitions. Appends at or above
    /// it are rejected with <see cref="AppendStatus.RejectedStreamFull"/> until
    /// retention frees sealed segments. Unlimited when null.
    /// </summary>
    public long? MaxTotalBytes { get; init; }

    public RetentionOptions Retention { get; init; } = new();
}

/// <summary>
/// Topic-level retention policy. Retention is independent of subscription
/// progress: a lagging subscription may fall behind and enter
/// <see cref="SubscriptionState.OffsetExpired"/>.
/// </summary>
public sealed class RetentionOptions
{
    /// <summary>Per-partition size cap. Oldest sealed segments are deleted first.</summary>
    public long? MaxBytes { get; init; }

    /// <summary>Sealed segments whose newest data is older than this are deleted.</summary>
    public TimeSpan? MaxAge { get; init; }
}
