using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>Canonical identity of one materialized result row.</summary>
public sealed record ResultKey
{
    public ResultKey(string canonicalKey, WindowInterval? window = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(canonicalKey);
        CanonicalKey = canonicalKey;
        Window = window;
    }

    public string CanonicalKey { get; }
    public WindowInterval? Window { get; }
}
