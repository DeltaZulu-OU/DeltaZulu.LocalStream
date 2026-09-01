using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace DeltaZulu.LocalStream.Query.Results;

/// <summary>Computes a stable identity from canonical, length-delimited fields.</summary>
public static class ResultIdentityBuilder
{
    private const int FormatVersion = 1;

    public static ResultChangeId Build(ResultIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendInt32(hash, FormatVersion);
        AppendString(hash, identity.QueryId);
        AppendInt64(hash, identity.Revision);
        AppendString(hash, identity.OutputBindingId);
        AppendString(hash, identity.OperatorId);
        AppendString(hash, identity.CanonicalResultKey);
        if (identity.Window is { } window)
        {
            AppendByte(hash, 1);
            AppendInt64(hash, window.StartUtc.UtcTicks);
            AppendInt64(hash, window.EndUtc.UtcTicks);
        }
        else
        {
            AppendByte(hash, 0);
        }

        AppendInt64(hash, identity.LogicalVersion);
        AppendString(hash, identity.Causality.Topic);
        AppendInt32(hash, identity.Causality.Partition);
        AppendInt64(hash, identity.Causality.StartOffset);
        AppendInt64(hash, identity.Causality.EndOffset);
        return ResultChangeId.FromHash(hash.GetHashAndReset());
    }

    private static void AppendString(IncrementalHash hash, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        AppendInt32(hash, bytes.Length);
        hash.AppendData(bytes);
    }

    private static void AppendInt32(IncrementalHash hash, int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        hash.AppendData(buffer);
    }

    private static void AppendInt64(IncrementalHash hash, long value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        hash.AppendData(buffer);
    }

    private static void AppendByte(IncrementalHash hash, byte value)
    {
        Span<byte> buffer = stackalloc byte[1];
        buffer[0] = value;
        hash.AppendData(buffer);
    }
}
