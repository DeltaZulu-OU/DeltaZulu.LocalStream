namespace DeltaZulu.LocalStream.Query.State;

/// <summary>Exclusive mutation domain for one query revision and partition assignment.</summary>
public sealed record StateDomainId
{
    public StateDomainId(string queryId, long revision, string partitionAssignment)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queryId);
        ArgumentOutOfRangeException.ThrowIfNegative(revision);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionAssignment);
        QueryId = queryId;
        Revision = revision;
        PartitionAssignment = partitionAssignment;
    }

    public string QueryId { get; }
    public long Revision { get; }
    public string PartitionAssignment { get; }
}
