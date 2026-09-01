using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.State;

public interface IStateTransaction : IAsyncDisposable
{
    StateDomainId Domain { get; }
    SourceRange SourceRange { get; }
    bool IsAlreadyCommitted { get; }

    ValueTask<ReadOnlyMemory<byte>?> GetOperatorStateAsync(
        StateKey key,
        CancellationToken cancellationToken = default);

    void PutOperatorState(StateKey key, ReadOnlyMemory<byte> value);
    void DeleteOperatorState(StateKey key);
    void SetWatermark(WatermarkState state);
    void AddOutputIntent(OutputIntent intent);

    ValueTask<CheckpointManifest> CommitAsync(
        long candidateCursorOffset,
        CancellationToken cancellationToken = default);
}
