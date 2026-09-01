namespace DeltaZulu.LocalStream.Query.State;

using DeltaZulu.LocalStream.Query.Results;

/// <summary>Exact durable output staged before an external delivery attempt.</summary>
public sealed record OutputIntent
{
    public OutputIntent(ResultChangeId resultChangeId, string target, ReadOnlyMemory<byte> payload)
    {
        if (resultChangeId == default)
        {
            throw new ArgumentException("A result change identity is required.", nameof(resultChangeId));
        }
        ArgumentException.ThrowIfNullOrWhiteSpace(target);
        ResultChangeId = resultChangeId;
        Target = target;
        Payload = payload;
    }

    public ResultChangeId ResultChangeId { get; }
    public string Target { get; }
    public ReadOnlyMemory<byte> Payload { get; init; }
}
