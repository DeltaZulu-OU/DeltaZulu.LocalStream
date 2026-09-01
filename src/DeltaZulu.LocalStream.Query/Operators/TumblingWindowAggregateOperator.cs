using DeltaZulu.LocalStream.Query.Results;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Operators;

public sealed record TumblingAggregateProcessResult(
    LateEventDecision LateEvent,
    QueryChange? Change);

/// <summary>
/// Executes already-bound events against transactional keyed tumbling state.
/// Query parsing and expression evaluation deliberately remain outside.
/// </summary>
public sealed class TumblingWindowAggregateOperator
{
    private readonly string _operatorId;
    private readonly string _outputBindingId;
    private readonly string _outputTarget;
    private readonly IWindowAssigner _windowAssigner;
    private readonly LateEventEvaluator _lateEvents;
    private readonly ExactDistinctPolicy _distinctPolicy;

    public TumblingWindowAggregateOperator(
        string operatorId,
        string outputBindingId,
        string outputTarget,
        IWindowAssigner windowAssigner,
        LateEventPolicy lateEventPolicy,
        ExactDistinctPolicy distinctPolicy)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operatorId);
        ArgumentException.ThrowIfNullOrWhiteSpace(outputBindingId);
        ArgumentException.ThrowIfNullOrWhiteSpace(outputTarget);
        _operatorId = operatorId;
        _outputBindingId = outputBindingId;
        _outputTarget = outputTarget;
        _windowAssigner = windowAssigner ?? throw new ArgumentNullException(nameof(windowAssigner));
        _lateEvents = new LateEventEvaluator(lateEventPolicy);
        _distinctPolicy = distinctPolicy ?? throw new ArgumentNullException(nameof(distinctPolicy));
    }

    public async ValueTask<TumblingAggregateProcessResult> ProcessAsync(
        IStateTransaction transaction,
        TumblingAggregateInput input,
        DateTimeOffset? watermarkUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(input);
        EnsureWritable(transaction);
        var window = _windowAssigner.Assign(input.EventTimeUtc);
        var lateDecision = _lateEvents.Evaluate(new LateEventContext(input.EventTimeUtc, watermarkUtc, window));
        if (!lateDecision.CanUpdateWindow)
        {
            return new TumblingAggregateProcessResult(lateDecision, null);
        }

        var stateKey = StateKey(input.Partition, input.LogicalKey, window);
        var state = await TumblingAggregateState.LoadAsync(
            transaction,
            stateKey,
            _distinctPolicy,
            cancellationToken).ConfigureAwait(false);
        state.AddEvent(input.CanonicalDistinctValue.Span);
        state.Save(transaction, stateKey);
        var kind = state.LogicalVersion == 1 ? QueryChangeKind.Upsert : QueryChangeKind.Correction;
        var change = CreateChange(transaction, input.LogicalKey, window, state, kind);
        StageOutput(transaction, change);
        return new TumblingAggregateProcessResult(lateDecision, change);
    }

    public async ValueTask<QueryChange?> FinalizeAsync(
        IStateTransaction transaction,
        int partition,
        string logicalKey,
        WindowInterval window,
        DateTimeOffset watermarkUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalKey);
        EnsureWritable(transaction);
        if (!_lateEvents.IsFinalized(window, watermarkUtc))
        {
            return null;
        }

        var stateKey = StateKey(partition, logicalKey, window);
        var payload = await transaction.GetOperatorStateAsync(stateKey, cancellationToken).ConfigureAwait(false);
        if (!payload.HasValue)
        {
            return null;
        }

        var state = TumblingAggregateState.Restore(payload.Value.Span, _distinctPolicy);
        if (!state.FinalizeWindow())
        {
            return null;
        }

        state.Save(transaction, stateKey);
        var change = CreateChange(transaction, logicalKey, window, state, QueryChangeKind.Finalize);
        StageOutput(transaction, change);
        return change;
    }

    private QueryChange CreateChange(
        IStateTransaction transaction,
        string logicalKey,
        WindowInterval window,
        TumblingAggregateState state,
        QueryChangeKind kind)
    {
        var resultKey = new ResultKey(logicalKey, window);
        var identity = ResultIdentityBuilder.Build(new ResultIdentity(
            transaction.Domain.QueryId,
            transaction.Domain.Revision,
            _outputBindingId,
            _operatorId,
            resultKey.CanonicalKey,
            window,
            state.LogicalVersion,
            transaction.SourceRange));
        ReadOnlyMemory<byte>? value = kind == QueryChangeKind.Finalize
            ? default(ReadOnlyMemory<byte>?)
            : AggregateResultValueCodec.Serialize(state.EventCount, state.DistinctCount);
        return new QueryChange(
            identity,
            kind,
            resultKey,
            state.LogicalVersion,
            value,
            transaction.SourceRange);
    }

    private StateKey StateKey(int partition, string logicalKey, WindowInterval window) =>
        new(_operatorId, partition, logicalKey, window);

    private void StageOutput(IStateTransaction transaction, QueryChange change) =>
        transaction.AddOutputIntent(new OutputIntent(
            change.ChangeId,
            _outputTarget,
            QueryChangeCodec.Serialize(change)));

    private static void EnsureWritable(IStateTransaction transaction)
    {
        if (transaction.IsAlreadyCommitted)
        {
            throw new InvalidOperationException("An already committed source range cannot execute operators again.");
        }
    }
}
