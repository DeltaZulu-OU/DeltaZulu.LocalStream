using System.Runtime.CompilerServices;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.State;

/// <summary>Atomic in-memory reference implementation of the state protocol.</summary>
public sealed class InMemoryQueryStateStore : IQueryStateStore
{
    private readonly object _sync = new();
    private readonly Dictionary<StateDomainId, DomainState> _domains = [];
    private readonly HashSet<StateDomainId> _writers = [];

    public ValueTask<IStateTransaction> BeginTransactionAsync(
        StateDomainId domain,
        SourceRange range,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(range);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            if (!_writers.Add(domain))
            {
                throw new InvalidOperationException($"State domain '{domain}' already has an active writer.");
            }

            var state = GetOrCreate(domain);
            var overlap = state.CommittedRanges.FirstOrDefault(committed =>
                committed.Topic == range.Topic
                && committed.Partition == range.Partition
                && committed.StartOffset <= range.EndOffset
                && range.StartOffset <= committed.EndOffset);
            if (overlap is not null && overlap != range)
            {
                _writers.Remove(domain);
                throw new InvalidOperationException(
                    $"Source range {range.StartOffset}..{range.EndOffset} overlaps committed range " +
                    $"{overlap.StartOffset}..{overlap.EndOffset}.");
            }

            return ValueTask.FromResult<IStateTransaction>(new Transaction(
                this,
                domain,
                range,
                state.CommittedRanges.Contains(range)));
        }
    }

    public ValueTask<CheckpointManifest?> GetCheckpointAsync(
        StateDomainId domain,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            return ValueTask.FromResult(_domains.TryGetValue(domain, out var state) ? state.Checkpoint : null);
        }
    }

    public async IAsyncEnumerable<OutputIntent> ReadPendingOutputIntentsAsync(
        StateDomainId domain,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        OutputIntent[] snapshot;
        lock (_sync)
        {
            snapshot = _domains.TryGetValue(domain, out var state)
                ? state.OutputIntents.Values.Select(Clone).ToArray()
                : [];
        }

        foreach (var intent in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return intent;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private DomainState GetOrCreate(StateDomainId domain)
    {
        if (!_domains.TryGetValue(domain, out var state))
        {
            state = new DomainState();
            _domains.Add(domain, state);
        }

        return state;
    }

    private void Release(StateDomainId domain)
    {
        lock (_sync)
        {
            _writers.Remove(domain);
        }
    }

    private static OutputIntent Clone(OutputIntent intent) =>
        intent with { Payload = intent.Payload.ToArray() };

    private sealed class DomainState
    {
        public Dictionary<StateKey, byte[]> OperatorState { get; } = [];
        public Dictionary<string, OutputIntent> OutputIntents { get; } = new(StringComparer.Ordinal);
        public HashSet<SourceRange> CommittedRanges { get; } = [];
        public CheckpointManifest? Checkpoint { get; set; }
    }

    private sealed class Transaction(
        InMemoryQueryStateStore store,
        StateDomainId domain,
        SourceRange sourceRange,
        bool isAlreadyCommitted) : IStateTransaction
    {
        private readonly Dictionary<StateKey, byte[]?> _stateChanges = [];
        private readonly Dictionary<string, OutputIntent> _outputIntents = new(StringComparer.Ordinal);
        private WatermarkState? _watermark;
        private bool _completed;

        public StateDomainId Domain { get; } = domain;
        public SourceRange SourceRange { get; } = sourceRange;
        public bool IsAlreadyCommitted { get; } = isAlreadyCommitted;

        public ValueTask<ReadOnlyMemory<byte>?> GetOperatorStateAsync(
            StateKey key,
            CancellationToken cancellationToken = default)
        {
            EnsureActive();
            cancellationToken.ThrowIfCancellationRequested();
            if (_stateChanges.TryGetValue(key, out var staged))
            {
                return staged is null
                    ? ValueTask.FromResult<ReadOnlyMemory<byte>?>(null)
                    : ValueTask.FromResult<ReadOnlyMemory<byte>?>(staged.AsMemory());
            }

            lock (store._sync)
            {
                var state = store.GetOrCreate(Domain);
                return state.OperatorState.TryGetValue(key, out var value)
                    ? ValueTask.FromResult<ReadOnlyMemory<byte>?>(value.ToArray().AsMemory())
                    : ValueTask.FromResult<ReadOnlyMemory<byte>?>(null);
            }
        }

        public void PutOperatorState(StateKey key, ReadOnlyMemory<byte> value)
        {
            EnsureMutable();
            _stateChanges[key] = value.ToArray();
        }

        public void DeleteOperatorState(StateKey key)
        {
            EnsureMutable();
            _stateChanges[key] = null;
        }

        public void SetWatermark(WatermarkState state)
        {
            EnsureMutable();
            _watermark = state ?? throw new ArgumentNullException(nameof(state));
        }

        public void AddOutputIntent(OutputIntent intent)
        {
            EnsureMutable();
            ArgumentNullException.ThrowIfNull(intent);
            if (!_outputIntents.TryAdd(intent.ResultChangeId, Clone(intent)))
            {
                throw new InvalidOperationException($"Output intent '{intent.ResultChangeId}' is duplicated in this transaction.");
            }
        }

        public ValueTask<CheckpointManifest> CommitAsync(
            long candidateCursorOffset,
            CancellationToken cancellationToken = default)
        {
            EnsureActive();
            cancellationToken.ThrowIfCancellationRequested();
            if (candidateCursorOffset != SourceRange.EndOffset)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(candidateCursorOffset),
                    candidateCursorOffset,
                    "The candidate cursor must equal the transaction range end.");
            }

            lock (store._sync)
            {
                var state = store.GetOrCreate(Domain);
                if (!IsAlreadyCommitted)
                {
                    foreach (var (id, intent) in _outputIntents)
                    {
                        if (state.OutputIntents.TryGetValue(id, out var existing)
                            && (!StringComparer.Ordinal.Equals(existing.Target, intent.Target)
                                || !existing.Payload.Span.SequenceEqual(intent.Payload.Span)))
                        {
                            throw new InvalidOperationException($"Output intent '{id}' has conflicting payloads.");
                        }
                    }

                    foreach (var (key, value) in _stateChanges)
                    {
                        if (value is null)
                        {
                            state.OperatorState.Remove(key);
                        }
                        else
                        {
                            state.OperatorState[key] = value;
                        }
                    }

                    foreach (var (id, intent) in _outputIntents)
                    {
                        state.OutputIntents[id] = intent;
                    }

                    var generation = (state.Checkpoint?.Generation ?? 0) + 1;
                    state.Checkpoint = new CheckpointManifest(
                        Domain,
                        SourceRange,
                        candidateCursorOffset,
                        _watermark,
                        generation);
                    state.CommittedRanges.Add(SourceRange);
                }

                _completed = true;
                store._writers.Remove(Domain);
                return ValueTask.FromResult(state.Checkpoint!);
            }
        }

        public ValueTask DisposeAsync()
        {
            if (!_completed)
            {
                _completed = true;
                store.Release(Domain);
            }

            return ValueTask.CompletedTask;
        }

        private void EnsureActive()
        {
            if (_completed)
            {
                throw new InvalidOperationException("The state transaction is already complete.");
            }
        }

        private void EnsureMutable()
        {
            EnsureActive();
            if (IsAlreadyCommitted)
            {
                throw new InvalidOperationException("An already committed source range cannot be mutated again.");
            }
        }
    }
}
