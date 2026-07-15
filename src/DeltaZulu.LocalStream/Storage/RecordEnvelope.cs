using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaZulu.LocalStream.Storage;

/// <summary>On-disk representation of one record inside a segment line.</summary>
internal sealed class RecordEnvelope
{
    [JsonPropertyName("offset")]
    public required long Offset { get; init; }

    [JsonPropertyName("eventId")]
    public required string EventId { get; init; }

    [JsonPropertyName("publishedUtc")]
    public required DateTimeOffset PublishedUtc { get; init; }

    [JsonPropertyName("headers")]
    public Dictionary<string, string> Headers { get; init; } = [];

    [JsonPropertyName("payload")]
    public required JsonElement Payload { get; init; }
}
