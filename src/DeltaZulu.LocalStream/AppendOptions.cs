namespace DeltaZulu.LocalStream;

/// <summary>Per-append options controlling partition placement and record identity.</summary>
public sealed class AppendOptions
{
    /// <summary>Explicit target partition. Takes precedence over <see cref="PartitionKey"/>.</summary>
    public int? Partition { get; init; }

    /// <summary>
    /// Stable routing key. Records with the same key always land in the same
    /// partition, preserving their relative order.
    /// </summary>
    public string? PartitionKey { get; init; }

    /// <summary>Caller-supplied stable event identity. Generated when omitted.</summary>
    public string? EventId { get; init; }

    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
