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
}
