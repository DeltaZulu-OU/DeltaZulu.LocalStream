using System.Buffers.Binary;

namespace DeltaZulu.LocalStream.Query.Operators;

/// <summary>Canonical value payload emitted by count/exact-distinct aggregates.</summary>
public static class AggregateResultValueCodec
{
    private const uint Magic = 0x445A4152; // DZAR
    private const int FormatVersion = 1;
    private const int PayloadBytes = 20;

    public static byte[] Serialize(long eventCount, int distinctCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(eventCount);
        ArgumentOutOfRangeException.ThrowIfNegative(distinctCount);
        var payload = new byte[PayloadBytes];
        BinaryPrimitives.WriteUInt32BigEndian(payload, Magic);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(4), FormatVersion);
        BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(8), eventCount);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(16), distinctCount);
        return payload;
    }

    public static (long EventCount, int DistinctCount) Deserialize(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != PayloadBytes
            || BinaryPrimitives.ReadUInt32BigEndian(payload) != Magic
            || BinaryPrimitives.ReadInt32BigEndian(payload[4..]) != FormatVersion)
        {
            throw new InvalidDataException("Aggregate result value has an unsupported format.");
        }

        var eventCount = BinaryPrimitives.ReadInt64BigEndian(payload[8..]);
        var distinctCount = BinaryPrimitives.ReadInt32BigEndian(payload[16..]);
        if (eventCount < 0 || distinctCount < 0)
        {
            throw new InvalidDataException("Aggregate result value contains negative counts.");
        }

        return (eventCount, distinctCount);
    }
}
