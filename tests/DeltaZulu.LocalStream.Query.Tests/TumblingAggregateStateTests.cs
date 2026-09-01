using System.Buffers.Binary;
using System.Text;
using DeltaZulu.LocalStream.Query.Operators;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class TumblingAggregateStateTests
{
    private static readonly ExactDistinctPolicy Policy = new(10, 1024);

    [TestMethod]
    public void AddEvent_UpdatesCountDistinctAndLogicalVersion()
    {
        var state = new TumblingAggregateState(Policy);

        Assert.AreEqual(ExactDistinctAddOutcome.Added, state.AddEvent(Bytes("alice")));
        Assert.AreEqual(ExactDistinctAddOutcome.Duplicate, state.AddEvent(Bytes("alice")));
        Assert.AreEqual(ExactDistinctAddOutcome.Added, state.AddEvent(Bytes("bob")));

        Assert.AreEqual(3, state.EventCount);
        Assert.AreEqual(2, state.DistinctCount);
        Assert.AreEqual(3, state.LogicalVersion);
    }

    [TestMethod]
    public void BudgetFailure_LeavesWholeAggregateUnchanged()
    {
        var state = new TumblingAggregateState(new ExactDistinctPolicy(1, 100));
        state.AddEvent(Bytes("alice"));
        var before = state.CaptureState();

        Assert.ThrowsExactly<ExactDistinctBudgetExceededException>(() => state.AddEvent(Bytes("bob")));

        Assert.AreEqual(1, state.EventCount);
        Assert.AreEqual(1, state.LogicalVersion);
        CollectionAssert.AreEqual(before, state.CaptureState());
    }

    [TestMethod]
    public void Finalize_IsIdempotentAndPreventsReopening()
    {
        var state = new TumblingAggregateState(Policy);
        state.AddEvent(Bytes("alice"));

        Assert.IsTrue(state.FinalizeWindow());
        Assert.IsFalse(state.FinalizeWindow());
        Assert.IsTrue(state.IsFinalized);
        Assert.AreEqual(2, state.LogicalVersion);
        Assert.ThrowsExactly<InvalidOperationException>(() => state.AddEvent(Bytes("bob")));
    }

    [TestMethod]
    public void CaptureRestore_IsCanonicalAndPreservesFinalization()
    {
        var first = new TumblingAggregateState(Policy);
        first.AddEvent(Bytes("bob"));
        first.AddEvent(Bytes("alice"));
        first.FinalizeWindow();
        var second = new TumblingAggregateState(Policy);
        second.AddEvent(Bytes("alice"));
        second.AddEvent(Bytes("bob"));
        second.FinalizeWindow();

        CollectionAssert.AreEqual(first.CaptureState(), second.CaptureState());
        var restored = TumblingAggregateState.Restore(first.CaptureState(), Policy);
        Assert.AreEqual(2, restored.EventCount);
        Assert.AreEqual(2, restored.DistinctCount);
        Assert.AreEqual(3, restored.LogicalVersion);
        Assert.IsTrue(restored.IsFinalized);
    }

    [TestMethod]
    public async Task LoadAndSave_UseWindowScopedTransactionalState()
    {
        var store = new InMemoryQueryStateStore();
        var domain = new StateDomainId("query", 1, "p0");
        var range = new SourceRange("events", 0, 0, 9);
        var window = new WindowInterval(DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddMinutes(5));
        var key = new StateKey("window-aggregate", 0, "group", window);
        await using (var transaction = await store.BeginTransactionAsync(domain, range))
        {
            var state = await TumblingAggregateState.LoadAsync(transaction, key, Policy);
            state.AddEvent(Bytes("alice"));
            state.Save(transaction, key);
            await transaction.CommitAsync(range.EndOffset);
        }

        await using var next = await store.BeginTransactionAsync(domain, new SourceRange("events", 0, 10, 19));
        var restored = await TumblingAggregateState.LoadAsync(next, key, Policy);
        Assert.AreEqual(1, restored.EventCount);
        Assert.AreEqual(1, restored.DistinctCount);
    }

    [TestMethod]
    public void Restore_RejectsUnknownTruncatedAndInconsistentState()
    {
        var state = new TumblingAggregateState(Policy);
        state.AddEvent(Bytes("alice"));
        var unknown = state.CaptureState();
        BinaryPrimitives.WriteInt32BigEndian(unknown.AsSpan(4), 2);
        Assert.ThrowsExactly<InvalidDataException>(() => TumblingAggregateState.Restore(unknown, Policy));

        var truncated = state.CaptureState();
        Assert.ThrowsExactly<InvalidDataException>(() =>
            TumblingAggregateState.Restore(truncated.AsSpan(0, truncated.Length - 1), Policy));

        var inconsistent = state.CaptureState();
        BinaryPrimitives.WriteInt64BigEndian(inconsistent.AsSpan(16), 0);
        Assert.ThrowsExactly<InvalidDataException>(() => TumblingAggregateState.Restore(inconsistent, Policy));
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);
}
