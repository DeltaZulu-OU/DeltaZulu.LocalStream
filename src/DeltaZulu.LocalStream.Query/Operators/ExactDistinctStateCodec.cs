using System.Buffers.Binary;

namespace DeltaZulu.LocalStream.Query.Operators;

/// <summary>Versioned canonical codec for exact distinct set state.</summary>
public static class ExactDistinctStateCodec
{
    private const uint Magic = 0x445A4344; // DZCD
    private const int FormatVersion = 1;
    private const int HeaderBytes = 12;

    internal static byte[] Serialize(IEnumerable<byte[]> orderedValues)
    {
        var values = orderedValues.ToArray();
        var length = checked(HeaderBytes + values.Sum(value => checked(sizeof(int) + value.Length)));
        var payload = new byte[length];
        BinaryPrimitives.WriteUInt32BigEndian(payload, Magic);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(4), FormatVersion);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(8), values.Length);
        var position = HeaderBytes;
        foreach (var value in values)
        {
            BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(position), value.Length);
            position += sizeof(int);
            value.CopyTo(payload, position);
            position += value.Length;
        }

        return payload;
    }

    internal static IReadOnlyList<byte[]> Deserialize(ReadOnlySpan<byte> payload, ExactDistinctPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        if (payload.Length < HeaderBytes
            || BinaryPrimitives.ReadUInt32BigEndian(payload) != Magic
            || BinaryPrimitives.ReadInt32BigEndian(payload[4..]) != FormatVersion)
        {
            throw new InvalidDataException("Exact distinct state has an unsupported format.");
        }

        if (payload.Length > policy.MaxStateBytes)
        {
            throw new ExactDistinctBudgetExceededException(ExactDistinctBudgetKind.StateBytes, policy.MaxStateBytes);
        }

        var count = BinaryPrimitives.ReadInt32BigEndian(payload[8..]);
        if (count < 0)
        {
            throw new InvalidDataException("Exact distinct state contains a negative count.");
        }

        if (count > policy.MaxCardinality)
        {
            throw new ExactDistinctBudgetExceededException(ExactDistinctBudgetKind.Cardinality, policy.MaxCardinality);
        }

        var values = new List<byte[]>(count);
        var position = HeaderBytes;
        byte[]? previous = null;
        for (var index = 0; index < count; index++)
        {
            if (payload.Length - position < sizeof(int))
            {
                throw new InvalidDataException("Exact distinct state is truncated.");
            }

            var valueLength = BinaryPrimitives.ReadInt32BigEndian(payload[position..]);
            position += sizeof(int);
            if (valueLength < 0 || payload.Length - position < valueLength)
            {
                throw new InvalidDataException("Exact distinct state contains an invalid value length.");
            }

            var value = payload.Slice(position, valueLength).ToArray();
            position += valueLength;
            if (previous is not null && LexicographicByteComparer.Instance.Compare(previous, value) >= 0)
            {
                throw new InvalidDataException("Exact distinct values are not in canonical order.");
            }

            values.Add(value);
            previous = value;
        }

        if (position != payload.Length)
        {
            throw new InvalidDataException("Exact distinct state contains trailing data.");
        }

        return values;
    }
}

internal sealed class LexicographicByteComparer : IComparer<byte[]>
{
    public static LexicographicByteComparer Instance { get; } = new();

    public int Compare(byte[]? left, byte[]? right)
    {
        if (ReferenceEquals(left, right)) return 0;
        if (left is null) return -1;
        if (right is null) return 1;
        var common = Math.Min(left.Length, right.Length);
        for (var index = 0; index < common; index++)
        {
            var comparison = left[index].CompareTo(right[index]);
            if (comparison != 0) return comparison;
        }

        return left.Length.CompareTo(right.Length);
    }
}
