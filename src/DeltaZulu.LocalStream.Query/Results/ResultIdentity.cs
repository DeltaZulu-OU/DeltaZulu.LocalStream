using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>Canonical semantic components of a result change identity.</summary>
public sealed record ResultIdentity
{
    public ResultIdentity(
        string queryId,
        long revision,
        string outputBindingId,
        string operatorId,
        string canonicalResultKey,
        WindowInterval? window,
        long logicalVersion,
        SourceRange causality)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queryId);
        ArgumentOutOfRangeException.ThrowIfNegative(revision);
        ArgumentException.ThrowIfNullOrWhiteSpace(outputBindingId);
        ArgumentException.ThrowIfNullOrWhiteSpace(operatorId);
        ArgumentNullException.ThrowIfNull(canonicalResultKey);
        ArgumentOutOfRangeException.ThrowIfNegative(logicalVersion);
        ArgumentNullException.ThrowIfNull(causality);
        QueryId = queryId;
        Revision = revision;
        OutputBindingId = outputBindingId;
        OperatorId = operatorId;
        CanonicalResultKey = canonicalResultKey;
        Window = window;
        LogicalVersion = logicalVersion;
        Causality = causality;
    }

    public string QueryId { get; }
    public long Revision { get; }
    public string OutputBindingId { get; }
    public string OperatorId { get; }
    public string CanonicalResultKey { get; }
    public WindowInterval? Window { get; }
    public long LogicalVersion { get; }
    public SourceRange Causality { get; }
}
