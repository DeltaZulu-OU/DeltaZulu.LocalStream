namespace DeltaZulu.LocalStream;

public sealed class LocalStreamOptions
{
    /// <summary>Root directory of the local stream store.</summary>
    public required string StoragePath { get; init; }

    public IList<TopicOptions> Topics { get; } = [];
}

public sealed class TopicOptions
{
    public required string Name { get; init; }

    public int Partitions { get; init; } = 1;

    /// <summary>Segment roll threshold. A new segment starts once the active one reaches this size.</summary>
    public long MaxSegmentBytes { get; init; } = 128 * 1024 * 1024;

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
