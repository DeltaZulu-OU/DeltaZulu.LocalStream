namespace DeltaZulu.LocalStream;

public enum SubscriptionState
{
    /// <summary>The subscription's next read position points at retained data.</summary>
    Active,

    /// <summary>
    /// Retention deleted records past the subscription's committed offset before
    /// they were read. The subscription must be reset to earliest, latest, a
    /// specific offset, or a timestamp before it can read again.
    /// </summary>
    OffsetExpired,
}
