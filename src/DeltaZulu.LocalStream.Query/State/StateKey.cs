using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.State;

/// <summary>Logical identity for one operator-state value.</summary>
public sealed record StateKey
{
    public StateKey(string operatorId, int partition, string logicalKey, WindowInterval? window = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operatorId);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentNullException.ThrowIfNull(logicalKey);
        OperatorId = operatorId;
        Partition = partition;
        LogicalKey = logicalKey;
        Window = window;
    }

    public string OperatorId { get; }
    public int Partition { get; }
    public string LogicalKey { get; }
    public WindowInterval? Window { get; }
}
