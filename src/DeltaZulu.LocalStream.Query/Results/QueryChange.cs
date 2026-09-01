using DeltaZulu.LocalStream.Query.State;

namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>One deterministic logical change emitted by a continuous query.</summary>
public sealed record QueryChange
{
    public QueryChange(
        ResultChangeId changeId,
        QueryChangeKind kind,
        ResultKey key,
        long version,
        ReadOnlyMemory<byte>? value,
        SourceRange causality)
    {
        if (changeId == default) throw new ArgumentException("A change identity is required.", nameof(changeId));
        if (!Enum.IsDefined(kind)) throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown query change kind.");
        ArgumentNullException.ThrowIfNull(key);
        ArgumentOutOfRangeException.ThrowIfNegative(version);
        ArgumentNullException.ThrowIfNull(causality);
        if (kind is QueryChangeKind.Upsert or QueryChangeKind.Correction && value is null)
        {
            throw new ArgumentException("Upserts and corrections require a value.", nameof(value));
        }

        if (kind is QueryChangeKind.Delete or QueryChangeKind.Finalize && value is not null)
        {
            throw new ArgumentException("Deletes and finalizations do not carry a replacement value.", nameof(value));
        }

        ChangeId = changeId;
        Kind = kind;
        Key = key;
        Version = version;
        Value = value.HasValue
            ? new ReadOnlyMemory<byte>(value.Value.ToArray())
            : default(ReadOnlyMemory<byte>?);
        Causality = causality;
    }

    public ResultChangeId ChangeId { get; }
    public QueryChangeKind Kind { get; }
    public ResultKey Key { get; }
    public long Version { get; }
    public ReadOnlyMemory<byte>? Value { get; }
    public SourceRange Causality { get; }
}
