using System.Buffers.Binary;
using System.Text;
using DeltaZulu.LocalStream.Query.Operators;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class ExactDistinctAccumulatorTests
{
    [TestMethod]
    public void Add_CountsCanonicalValuesExactlyAndIgnoresDuplicates()
    {
        var accumulator = Create();

        Assert.AreEqual(ExactDistinctAddOutcome.Added, accumulator.Add(Bytes("alice")));
        Assert.AreEqual(ExactDistinctAddOutcome.Added, accumulator.Add(Bytes("bob")));
        Assert.AreEqual(ExactDistinctAddOutcome.Duplicate, accumulator.Add(Bytes("alice")));
        Assert.AreEqual(2, accumulator.Count);
    }

    [TestMethod]
    public void CaptureState_IsByteStableRegardlessOfInsertionOrder()
    {
        var forward = Create();
        forward.Add(Bytes("alice"));
        forward.Add(Bytes("bob"));
        var reverse = Create();
        reverse.Add(Bytes("bob"));
        reverse.Add(Bytes("alice"));

        CollectionAssert.AreEqual(forward.CaptureState(), reverse.CaptureState());
        var restored = ExactDistinctAccumulator.Restore(forward.CaptureState(), Policy());
        Assert.AreEqual(2, restored.Count);
        CollectionAssert.AreEqual(forward.CaptureState(), restored.CaptureState());
    }

    [TestMethod]
    public void Add_EnforcesCardinalityWithoutMutatingState()
    {
        var accumulator = new ExactDistinctAccumulator(new ExactDistinctPolicy(1, 100));
        accumulator.Add(Bytes("alice"));
        var before = accumulator.CaptureState();

        var exception = Assert.ThrowsExactly<ExactDistinctBudgetExceededException>(
            () => accumulator.Add(Bytes("bob")));

        Assert.AreEqual(ExactDistinctBudgetKind.Cardinality, exception.Budget);
        Assert.AreEqual(1, accumulator.Count);
        CollectionAssert.AreEqual(before, accumulator.CaptureState());
    }

    [TestMethod]
    public void Add_EnforcesSerializedByteBudgetWithoutMutatingState()
    {
        var accumulator = new ExactDistinctAccumulator(new ExactDistinctPolicy(10, 20));
        accumulator.Add(new byte[4]); // 12-byte header + 4-byte length + 4-byte value.

        var exception = Assert.ThrowsExactly<ExactDistinctBudgetExceededException>(
            () => accumulator.Add(new byte[] { 1 }));

        Assert.AreEqual(ExactDistinctBudgetKind.StateBytes, exception.Budget);
        Assert.AreEqual(1, accumulator.Count);
        Assert.AreEqual(20, accumulator.SerializedBytes);
    }

    [TestMethod]
    public void Restore_RejectsUnknownTruncatedAndNonCanonicalState()
    {
        var valid = Create();
        valid.Add(Bytes("a"));
        valid.Add(Bytes("b"));
        var unknown = valid.CaptureState();
        BinaryPrimitives.WriteInt32BigEndian(unknown.AsSpan(4), 2);
        Assert.ThrowsExactly<InvalidDataException>(() =>
            ExactDistinctAccumulator.Restore(unknown, Policy()));

        Assert.ThrowsExactly<InvalidDataException>(() =>
        {
            var truncated = valid.CaptureState();
            ExactDistinctAccumulator.Restore(truncated.AsSpan(0, truncated.Length - 1), Policy());
        });

        var nonCanonical = valid.CaptureState();
        (nonCanonical[16], nonCanonical[21]) = (nonCanonical[21], nonCanonical[16]);
        Assert.ThrowsExactly<InvalidDataException>(() =>
            ExactDistinctAccumulator.Restore(nonCanonical, Policy()));
    }

    [TestMethod]
    public void EmptyCanonicalValue_IsARealDistinctValue()
    {
        var accumulator = Create();
        Assert.AreEqual(ExactDistinctAddOutcome.Added, accumulator.Add([]));
        Assert.AreEqual(ExactDistinctAddOutcome.Duplicate, accumulator.Add([]));
        Assert.AreEqual(1, accumulator.Count);
    }

    [TestMethod]
    public async Task CanonicalState_RoundTripsThroughTransactionalOperatorState()
    {
        var store = new InMemoryQueryStateStore();
        var domain = new StateDomainId("query", 1, "p0");
        var range = new SourceRange("events", 0, 0, 9);
        var key = new StateKey(
            "count-distinct",
            0,
            "group",
            new WindowInterval(DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddMinutes(5)));
        var accumulator = Create();
        accumulator.Add(Bytes("alice"));

        await using (var transaction = await store.BeginTransactionAsync(domain, range))
        {
            transaction.PutOperatorState(key, accumulator.CaptureState());
            await transaction.CommitAsync(range.EndOffset);
        }

        await using var next = await store.BeginTransactionAsync(
            domain,
            new SourceRange("events", 0, 10, 19));
        var payload = await next.GetOperatorStateAsync(key);
        var restored = ExactDistinctAccumulator.Restore(payload!.Value.Span, Policy());
        Assert.AreEqual(1, restored.Count);
        Assert.AreEqual(ExactDistinctAddOutcome.Duplicate, restored.Add(Bytes("alice")));
    }

    private static ExactDistinctAccumulator Create() => new(Policy());
    private static ExactDistinctPolicy Policy() => new(100, 4096);
    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);
}
