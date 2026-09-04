namespace DeltaZulu.LocalStream;

/// <summary>
/// Thrown when a subscription's checkpoint file exists but cannot be parsed —
/// for example a file truncated or left empty by a crash mid-write. This is
/// distinct from <see cref="OffsetExpiredException"/>: the checkpoint itself
/// is unreadable, not merely stale. The failed read is not cached, so
/// repairing or replacing the file and retrying recovers normally.
/// </summary>
public sealed class CorruptCheckpointException : InvalidOperationException
{
    public CorruptCheckpointException()
    {
    }

    public CorruptCheckpointException(string message)
        : base(message)
    {
    }

    public CorruptCheckpointException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public CorruptCheckpointException(
        string subscriptionId,
        string topic,
        int partition,
        string path,
        Exception innerException)
        : base(
            $"Checkpoint for subscription '{subscriptionId}' on '{topic}' partition {partition} at " +
            $"'{path}' could not be parsed. A checkpoint left renamed-but-empty or truncated by a " +
            "crash is the usual cause. Reset the subscription (which starts it over from its " +
            "configured StartPosition) or restore the file from backup before retrying.",
            innerException)
    {
        SubscriptionId = subscriptionId;
        Topic = topic;
        Partition = partition;
        Path = path;
    }

    public string? SubscriptionId { get; }
    public string? Topic { get; }
    public int Partition { get; }
    public string? Path { get; }
}
