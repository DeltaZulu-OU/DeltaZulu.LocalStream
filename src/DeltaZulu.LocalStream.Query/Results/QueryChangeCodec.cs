using System.Buffers.Binary;
using System.Text;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>Versioned canonical codec for durable query-change output intents.</summary>
public static class QueryChangeCodec
{
    private const uint Magic = 0x445A5143; // DZQC
    private const int FormatVersion = 1;
    private const int MaximumPayloadBytes = 16 * 1024 * 1024;
    private static readonly Encoding Utf8 = new UTF8Encoding(false, true);

    public static byte[] Serialize(QueryChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        using var stream = new MemoryStream();
        WriteUInt32(stream, Magic);
        WriteInt32(stream, FormatVersion);
        WriteString(stream, change.ChangeId.Value);
        WriteInt32(stream, (int)change.Kind);
        WriteString(stream, change.Key.CanonicalKey);
        WriteByte(stream, change.Key.Window.HasValue ? (byte)1 : (byte)0);
        if (change.Key.Window is { } window)
        {
            WriteInt64(stream, window.StartUtc.UtcTicks);
            WriteInt64(stream, window.EndUtc.UtcTicks);
        }

        WriteInt64(stream, change.Version);
        WriteByte(stream, change.Value.HasValue ? (byte)1 : (byte)0);
        if (change.Value is { } value)
        {
            WriteBytes(stream, value.Span);
        }

        WriteString(stream, change.Causality.Topic);
        WriteInt32(stream, change.Causality.Partition);
        WriteInt64(stream, change.Causality.StartOffset);
        WriteInt64(stream, change.Causality.EndOffset);
        if (stream.Length > MaximumPayloadBytes)
        {
            throw new InvalidOperationException($"Query change exceeds the {MaximumPayloadBytes}-byte codec limit.");
        }

        return stream.ToArray();
    }

    public static QueryChange Deserialize(ReadOnlySpan<byte> payload)
    {
        if (payload.Length > MaximumPayloadBytes)
        {
            throw new InvalidDataException("Query change payload exceeds the codec limit.");
        }

        var reader = new Reader(payload);
        if (reader.ReadUInt32() != Magic || reader.ReadInt32() != FormatVersion)
        {
            throw new InvalidDataException("Query change has an unsupported format.");
        }

        ResultChangeId changeId;
        try
        {
            changeId = ResultChangeId.Parse(reader.ReadString());
        }
        catch (FormatException exception)
        {
            throw new InvalidDataException("Query change contains an invalid identity.", exception);
        }

        var kindValue = reader.ReadInt32();
        if (!Enum.IsDefined((QueryChangeKind)kindValue))
        {
            throw new InvalidDataException("Query change contains an unknown operation.");
        }

        var canonicalKey = reader.ReadString();
        var hasWindow = reader.ReadBooleanFlag();
        WindowInterval? window = hasWindow
            ? new WindowInterval(
                new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero),
                new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero))
            : null;
        var version = reader.ReadInt64();
        var hasValue = reader.ReadBooleanFlag();
        ReadOnlyMemory<byte>? value = hasValue
            ? new ReadOnlyMemory<byte>(reader.ReadBytes())
            : default(ReadOnlyMemory<byte>?);
        var causality = new SourceRange(
            reader.ReadString(),
            reader.ReadInt32(),
            reader.ReadInt64(),
            reader.ReadInt64());
        reader.EnsureFinished();
        try
        {
            return new QueryChange(
                changeId,
                (QueryChangeKind)kindValue,
                new ResultKey(canonicalKey, window),
                version,
                value,
                causality);
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Query change violates its semantic contract.", exception);
        }
    }

    private static void WriteString(Stream stream, string value) => WriteBytes(stream, Utf8.GetBytes(value));

    private static void WriteBytes(Stream stream, ReadOnlySpan<byte> value)
    {
        WriteInt32(stream, value.Length);
        stream.Write(value);
    }

    private static void WriteByte(Stream stream, byte value) => stream.WriteByte(value);

    private static void WriteUInt32(Stream stream, uint value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteInt64(Stream stream, long value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private ref struct Reader(ReadOnlySpan<byte> payload)
    {
        private readonly ReadOnlySpan<byte> _payload = payload;
        private int _position;

        public uint ReadUInt32()
        {
            var value = Take(sizeof(uint));
            return BinaryPrimitives.ReadUInt32BigEndian(value);
        }

        public int ReadInt32()
        {
            var value = Take(sizeof(int));
            return BinaryPrimitives.ReadInt32BigEndian(value);
        }

        public long ReadInt64()
        {
            var value = Take(sizeof(long));
            return BinaryPrimitives.ReadInt64BigEndian(value);
        }

        public bool ReadBooleanFlag()
        {
            var value = Take(1)[0];
            return value switch
            {
                0 => false,
                1 => true,
                _ => throw new InvalidDataException("Query change contains an invalid boolean flag."),
            };
        }

        public string ReadString()
        {
            try
            {
                return Utf8.GetString(ReadBytes());
            }
            catch (DecoderFallbackException exception)
            {
                throw new InvalidDataException("Query change contains invalid UTF-8.", exception);
            }
        }

        public byte[] ReadBytes()
        {
            var length = ReadInt32();
            if (length < 0)
            {
                throw new InvalidDataException("Query change contains a negative field length.");
            }

            return Take(length).ToArray();
        }

        public void EnsureFinished()
        {
            if (_position != _payload.Length)
            {
                throw new InvalidDataException("Query change contains trailing data.");
            }
        }

        private ReadOnlySpan<byte> Take(int length)
        {
            if (length < 0 || _payload.Length - _position < length)
            {
                throw new InvalidDataException("Query change payload is truncated.");
            }

            var value = _payload.Slice(_position, length);
            _position += length;
            return value;
        }
    }
}
