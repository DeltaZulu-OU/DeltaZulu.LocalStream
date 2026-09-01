namespace DeltaZulu.LocalStream.Query.Operators;

/// <summary>Hard cardinality and serialized-state limits for exact distinct aggregation.</summary>
public sealed record ExactDistinctPolicy
{
    internal const int MinimumStateBytes = 12;

    public ExactDistinctPolicy(int maxCardinality, int maxStateBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCardinality, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxStateBytes, MinimumStateBytes);
        MaxCardinality = maxCardinality;
        MaxStateBytes = maxStateBytes;
    }

    public int MaxCardinality { get; }
    public int MaxStateBytes { get; }
}

public enum ExactDistinctBudgetKind
{
    Cardinality,
    StateBytes,
}

public sealed class ExactDistinctBudgetExceededException : InvalidOperationException
{
    public ExactDistinctBudgetExceededException(ExactDistinctBudgetKind budget, int limit)
        : base($"Exact distinct {budget} budget of {limit} was exceeded.")
    {
        Budget = budget;
        Limit = limit;
    }

    public ExactDistinctBudgetKind Budget { get; }
    public int Limit { get; }
}
