namespace DeltaZulu.LocalStream.Query.Operators;

public enum ExactDistinctAddOutcome
{
    Added,
    Duplicate,
}

/// <summary>Budgeted exact distinct accumulator over canonical value bytes.</summary>
public sealed class ExactDistinctAccumulator
{
    private readonly ExactDistinctPolicy _policy;
    private readonly SortedSet<byte[]> _values = new(LexicographicByteComparer.Instance);
    private int _serializedBytes = ExactDistinctPolicy.MinimumStateBytes;

    public ExactDistinctAccumulator(ExactDistinctPolicy policy)
    {
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
    }

    public int Count => _values.Count;
    public int SerializedBytes => _serializedBytes;

    public static ExactDistinctAccumulator Restore(
        ReadOnlySpan<byte> payload,
        ExactDistinctPolicy policy)
    {
        var accumulator = new ExactDistinctAccumulator(policy);
        foreach (var value in ExactDistinctStateCodec.Deserialize(payload, policy))
        {
            accumulator._values.Add(value);
            accumulator._serializedBytes = checked(accumulator._serializedBytes + sizeof(int) + value.Length);
        }

        return accumulator;
    }

    public ExactDistinctAddOutcome Add(ReadOnlySpan<byte> canonicalValue)
    {
        var value = canonicalValue.ToArray();
        if (_values.Contains(value))
        {
            return ExactDistinctAddOutcome.Duplicate;
        }

        if (_values.Count == _policy.MaxCardinality)
        {
            throw new ExactDistinctBudgetExceededException(
                ExactDistinctBudgetKind.Cardinality,
                _policy.MaxCardinality);
        }

        var nextBytes = checked(_serializedBytes + sizeof(int) + value.Length);
        if (nextBytes > _policy.MaxStateBytes)
        {
            throw new ExactDistinctBudgetExceededException(
                ExactDistinctBudgetKind.StateBytes,
                _policy.MaxStateBytes);
        }

        _values.Add(value);
        _serializedBytes = nextBytes;
        return ExactDistinctAddOutcome.Added;
    }

    public byte[] CaptureState() => ExactDistinctStateCodec.Serialize(_values);
}
