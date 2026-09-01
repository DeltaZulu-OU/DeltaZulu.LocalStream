namespace DeltaZulu.LocalStream.Query.State;

/// <summary>Exact durable output staged before an external delivery attempt.</summary>
public sealed record OutputIntent
{
    public OutputIntent(string resultChangeId, string target, ReadOnlyMemory<byte> payload)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resultChangeId);
        ArgumentException.ThrowIfNullOrWhiteSpace(target);
        ResultChangeId = resultChangeId;
        Target = target;
        Payload = payload;
    }

    public string ResultChangeId { get; }
    public string Target { get; }
    public ReadOnlyMemory<byte> Payload { get; init; }
}
