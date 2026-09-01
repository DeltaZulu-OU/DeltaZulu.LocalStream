namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>Current versioned state of one materialized result key.</summary>
public sealed record MaterializedResult(
    ResultKey Key,
    long Version,
    ReadOnlyMemory<byte>? Value,
    bool IsDeleted,
    bool IsFinalized,
    ResultChangeId LastChangeId);

public enum MaterializationOutcome
{
    Applied,
    Duplicate,
    Stale,
}
