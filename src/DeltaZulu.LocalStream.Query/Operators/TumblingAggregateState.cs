using System.Buffers.Binary;
using DeltaZulu.LocalStream.Query.State;

namespace DeltaZulu.LocalStream.Query.Operators;

/// <summary>
/// Versioned count and exact-distinct state for one keyed tumbling window.
/// </summary>
public sealed class TumblingAggregateState
{
    private const uint Magic = 0x445A5741; // DZWA
    private const int FormatVersion = 1;
    private const int HeaderBytes = 29;
    private readonly ExactDistinctPolicy _distinctPolicy;
    private ExactDistinctAccumulator _distinct;

    public TumblingAggregateState(ExactDistinctPolicy distinctPolicy)
    {
        _distinctPolicy = distinctPolicy ?? throw new ArgumentNullException(nameof(distinctPolicy));
        _distinct = new ExactDistinctAccumulator(distinctPolicy);
    }

    public long EventCount { get; private set; }
    public int DistinctCount => _distinct.Count;
    public long LogicalVersion { get; private set; }
    public bool IsFinalized { get; private set; }

    public ExactDistinctAddOutcome AddEvent(ReadOnlySpan<byte> canonicalDistinctValue)
    {
        if (IsFinalized)
        {
            throw new InvalidOperationException("A finalized tumbling window cannot be updated.");
        }

        if (EventCount == long.MaxValue || LogicalVersion == long.MaxValue)
        {
            throw new OverflowException("Tumbling aggregate counters cannot be incremented further.");
        }

        // ExactDistinctAccumulator checks every budget before mutating, so a
        // rejected distinct value leaves all aggregate counters unchanged.
        var outcome = _distinct.Add(canonicalDistinctValue);
        EventCount++;
        LogicalVersion++;
        return outcome;
    }

    public bool FinalizeWindow()
    {
        if (IsFinalized)
        {
            return false;
        }

        if (LogicalVersion == long.MaxValue)
        {
            throw new OverflowException("Tumbling aggregate version cannot be incremented further.");
        }

        IsFinalized = true;
        LogicalVersion++;
        return true;
    }

    public byte[] CaptureState()
    {
        var distinctState = _distinct.CaptureState();
        var payload = new byte[checked(HeaderBytes + distinctState.Length)];
        BinaryPrimitives.WriteUInt32BigEndian(payload, Magic);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(4), FormatVersion);
        BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(8), EventCount);
        BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(16), LogicalVersion);
        payload[24] = IsFinalized ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(25), distinctState.Length);
        distinctState.CopyTo(payload, HeaderBytes);
        return payload;
    }

    public static TumblingAggregateState Restore(
        ReadOnlySpan<byte> payload,
        ExactDistinctPolicy distinctPolicy)
    {
        ArgumentNullException.ThrowIfNull(distinctPolicy);
        if (payload.Length < HeaderBytes
            || BinaryPrimitives.ReadUInt32BigEndian(payload) != Magic
            || BinaryPrimitives.ReadInt32BigEndian(payload[4..]) != FormatVersion)
        {
            throw new InvalidDataException("Tumbling aggregate state has an unsupported format.");
        }

        var eventCount = BinaryPrimitives.ReadInt64BigEndian(payload[8..]);
        var logicalVersion = BinaryPrimitives.ReadInt64BigEndian(payload[16..]);
        if (eventCount < 0 || logicalVersion < 0 || payload[24] > 1)
        {
            throw new InvalidDataException("Tumbling aggregate state contains invalid counters or flags.");
        }

        var distinctLength = BinaryPrimitives.ReadInt32BigEndian(payload[25..]);
        if (distinctLength < 0 || payload.Length - HeaderBytes != distinctLength)
        {
            throw new InvalidDataException("Tumbling aggregate state contains an invalid distinct-state length.");
        }

        var state = new TumblingAggregateState(distinctPolicy)
        {
            EventCount = eventCount,
            LogicalVersion = logicalVersion,
            IsFinalized = payload[24] == 1,
            _distinct = ExactDistinctAccumulator.Restore(payload[HeaderBytes..], distinctPolicy),
        };
        if (state.IsFinalized && state.EventCount == long.MaxValue)
        {
            throw new InvalidDataException("Tumbling aggregate counters cannot represent finalization.");
        }

        var expectedVersion = state.EventCount + (state.IsFinalized ? 1 : 0);
        if (state.LogicalVersion != expectedVersion)
        {
            throw new InvalidDataException("Tumbling aggregate version is inconsistent with its lifecycle.");
        }

        return state;
    }

    public static async ValueTask<TumblingAggregateState> LoadAsync(
        IStateTransaction transaction,
        StateKey key,
        ExactDistinctPolicy distinctPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(key);
        var payload = await transaction.GetOperatorStateAsync(key, cancellationToken).ConfigureAwait(false);
        return payload.HasValue
            ? Restore(payload.Value.Span, distinctPolicy)
            : new TumblingAggregateState(distinctPolicy);
    }

    public void Save(IStateTransaction transaction, StateKey key)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(key);
        transaction.PutOperatorState(key, CaptureState());
    }
}
