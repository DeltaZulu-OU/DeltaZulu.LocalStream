namespace DeltaZulu.LocalStream.Query.State;

public interface IQueryStateStore
{
    ValueTask<IStateTransaction> BeginTransactionAsync(
        StateDomainId domain,
        SourceRange range,
        CancellationToken cancellationToken = default);

    ValueTask<CheckpointManifest?> GetCheckpointAsync(
        StateDomainId domain,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<OutputIntent> ReadPendingOutputIntentsAsync(
        StateDomainId domain,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a successfully delivered intent complete. Returns false when the
    /// identity was already absent, making repeated acknowledgements idempotent.
    /// </summary>
    ValueTask<bool> MarkOutputIntentDeliveredAsync(
        StateDomainId domain,
        string resultChangeId,
        CancellationToken cancellationToken = default);
}
