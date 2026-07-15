namespace DeltaZulu.LocalStream;

/// <summary>
/// Thrown when a subscription attempts to resume from a committed offset that
/// retention has already deleted. Reset the subscription to recover.
/// </summary>
public sealed class OffsetExpiredException : InvalidOperationException
{
    public OffsetExpiredException()
    {
    }

    public OffsetExpiredException(string message)
        : base(message)
    {
    }

    public OffsetExpiredException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public OffsetExpiredException(
        string subscriptionId,
        string topic,
        int partition,
        long committedOffset,
        long earliestRetainedOffset)
        : base(
            $"Subscription '{subscriptionId}' on '{topic}' partition {partition} committed offset " +
            $"{committedOffset}, but the earliest retained offset is {earliestRetainedOffset}. " +
            "Reset the subscription to earliest, latest, an offset, or a timestamp.")
    {
        SubscriptionId = subscriptionId;
        Topic = topic;
        Partition = partition;
        CommittedOffset = committedOffset;
        EarliestRetainedOffset = earliestRetainedOffset;
    }

    public string? SubscriptionId { get; }
    public string? Topic { get; }
    public int Partition { get; }
    public long CommittedOffset { get; }
    public long EarliestRetainedOffset { get; }
}
