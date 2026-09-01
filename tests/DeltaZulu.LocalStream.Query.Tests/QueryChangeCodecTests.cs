using System.Buffers.Binary;
using DeltaZulu.LocalStream.Query.Results;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class QueryChangeCodecTests
{
    private static readonly SourceRange Range = new("events", 2, 10, 19);
    private static readonly WindowInterval Window = new(
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch.AddMinutes(5));

    [TestMethod]
    public void RoundTrip_PreservesEveryUpsertFieldAndCanonicalBytes()
    {
        var change = Change(QueryChangeKind.Upsert, new byte[] { 1, 2, 3 });

        var first = QueryChangeCodec.Serialize(change);
        var restored = QueryChangeCodec.Deserialize(first);
        var second = QueryChangeCodec.Serialize(restored);

        Assert.AreEqual(change.ChangeId, restored.ChangeId);
        Assert.AreEqual(change.Kind, restored.Kind);
        Assert.AreEqual(change.Key, restored.Key);
        Assert.AreEqual(change.Version, restored.Version);
        Assert.AreEqual(change.Causality, restored.Causality);
        CollectionAssert.AreEqual(change.Value!.Value.ToArray(), restored.Value!.Value.ToArray());
        CollectionAssert.AreEqual(first, second);
    }

    [TestMethod]
    public void RoundTrip_PreservesDeleteAndFinalizeNullPayloads()
    {
        foreach (var kind in new[] { QueryChangeKind.Delete, QueryChangeKind.Finalize })
        {
            var restored = QueryChangeCodec.Deserialize(QueryChangeCodec.Serialize(Change(kind, null)));
            Assert.AreEqual(kind, restored.Kind);
            Assert.IsNull(restored.Value);
        }
    }

    [TestMethod]
    public void Deserialize_RejectsUnknownVersionTruncationTrailingDataAndInvalidFlags()
    {
        var valid = QueryChangeCodec.Serialize(Change(QueryChangeKind.Upsert, new byte[] { 1 }));
        var unknown = valid.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(unknown.AsSpan(4), 2);
        Assert.ThrowsExactly<InvalidDataException>(() => QueryChangeCodec.Deserialize(unknown));
        Assert.ThrowsExactly<InvalidDataException>(() => QueryChangeCodec.Deserialize(valid.AsSpan(0, valid.Length - 1)));
        Assert.ThrowsExactly<InvalidDataException>(() => QueryChangeCodec.Deserialize([.. valid, 0]));

        // Skip magic/version, identity, and kind to reach the key/window section.
        var invalidFlag = valid.ToArray();
        var identityLength = BinaryPrimitives.ReadInt32BigEndian(invalidFlag.AsSpan(8));
        var keyLengthOffset = 12 + identityLength + sizeof(int);
        var keyLength = BinaryPrimitives.ReadInt32BigEndian(invalidFlag.AsSpan(keyLengthOffset));
        invalidFlag[keyLengthOffset + sizeof(int) + keyLength] = 2;
        Assert.ThrowsExactly<InvalidDataException>(() => QueryChangeCodec.Deserialize(invalidFlag));
    }

    [TestMethod]
    public void Deserialize_RejectsSemanticallyInvalidValuePresence()
    {
        var delete = QueryChangeCodec.Serialize(Change(QueryChangeKind.Delete, null));
        // Locate the value-presence byte after the fixed window and version.
        var position = 8;
        position += sizeof(int) + BinaryPrimitives.ReadInt32BigEndian(delete.AsSpan(position));
        position += sizeof(int); // kind
        position += sizeof(int) + BinaryPrimitives.ReadInt32BigEndian(delete.AsSpan(position));
        position += 1 + sizeof(long) + sizeof(long) + sizeof(long);
        delete[position] = 1;
        // A present value then requires a length, so this mutation cannot be accepted.
        Assert.ThrowsExactly<InvalidDataException>(() => QueryChangeCodec.Deserialize(delete));
    }

    private static QueryChange Change(QueryChangeKind kind, byte[]? value)
    {
        var id = ResultIdentityBuilder.Build(new ResultIdentity(
            "query", 1, "results", "aggregate", "key", Window, 3, Range));
        ReadOnlyMemory<byte>? payload = value is null
            ? default(ReadOnlyMemory<byte>?)
            : new ReadOnlyMemory<byte>(value);
        return new QueryChange(id, kind, new ResultKey("key", Window), 3, payload, Range);
    }
}
