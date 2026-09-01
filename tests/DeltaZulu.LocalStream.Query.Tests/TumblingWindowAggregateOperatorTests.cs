using System.Text;
using DeltaZulu.LocalStream.Query.Operators;
using DeltaZulu.LocalStream.Query.Results;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class TumblingWindowAggregateOperatorTests
{
    private static readonly StateDomainId Domain = new("failed-auth", 1, "p0");
    private static readonly DateTimeOffset Start = DateTimeOffset.UnixEpoch;

    [TestMethod]
    public async Task Process_EmitsUpsertThenCorrectionWithExactCounts()
    {
        var store = new InMemoryQueryStateStore();
        await using var transaction = await store.BeginTransactionAsync(Domain, Range(0, 1));
        var operation = Operator();

        var first = await operation.ProcessAsync(transaction, Input(0, "ip", Start, "alice"), null);
        var second = await operation.ProcessAsync(transaction, Input(0, "ip", Start.AddMinutes(1), "alice"), null);

        Assert.AreEqual(QueryChangeKind.Upsert, first.Change!.Kind);
        Assert.AreEqual(QueryChangeKind.Correction, second.Change!.Kind);
        Assert.AreNotEqual(first.Change.ChangeId, second.Change.ChangeId);
        Assert.AreEqual((1L, 1), AggregateResultValueCodec.Deserialize(first.Change.Value!.Value.Span));
        Assert.AreEqual((2L, 1), AggregateResultValueCodec.Deserialize(second.Change.Value!.Value.Span));
        await transaction.CommitAsync(1);
        var intents = await ReadIntents(store);
        Assert.HasCount(2, intents);
        Assert.IsTrue(intents.All(intent => intent.Target == "query-results"));
        CollectionAssert.AreEquivalent(
            new[] { first.Change.ChangeId, second.Change.ChangeId },
            intents.Select(intent => QueryChangeCodec.Deserialize(intent.Payload.Span).ChangeId).ToArray());
        var materializer = new InMemoryQueryResultStore();
        foreach (var intent in intents)
        {
            await materializer.ApplyAsync(Domain, QueryChangeCodec.Deserialize(intent.Payload.Span));
            Assert.IsTrue(await store.MarkOutputIntentDeliveredAsync(Domain, intent.ResultChangeId));
        }

        var materialized = await materializer.GetAsync(Domain, second.Change.Key);
        Assert.IsNotNull(materialized);
        Assert.AreEqual(2, materialized.Version);
        Assert.HasCount(0, await ReadIntents(store));
    }

    [TestMethod]
    public async Task Process_AssignsExactEndToNextWindowAndIsolatesKeys()
    {
        var store = new InMemoryQueryStateStore();
        await using var transaction = await store.BeginTransactionAsync(Domain, Range(0, 2));
        var operation = Operator();

        var first = await operation.ProcessAsync(transaction, Input(0, "a", Start.AddMinutes(4), "user"), null);
        var boundary = await operation.ProcessAsync(transaction, Input(0, "a", Start.AddMinutes(5), "user"), null);
        var otherKey = await operation.ProcessAsync(transaction, Input(0, "b", Start.AddMinutes(4), "user"), null);

        Assert.AreEqual(Start, first.Change!.Key.Window!.Value.StartUtc);
        Assert.AreEqual(Start.AddMinutes(5), boundary.Change!.Key.Window!.Value.StartUtc);
        Assert.AreEqual(QueryChangeKind.Upsert, boundary.Change.Kind);
        Assert.AreEqual(QueryChangeKind.Upsert, otherKey.Change!.Kind);
    }

    [TestMethod]
    public async Task Process_DistinguishesLateAcceptedFromTooLate()
    {
        var store = new InMemoryQueryStateStore();
        await using var transaction = await store.BeginTransactionAsync(Domain, Range(0, 1));
        var operation = Operator();

        var accepted = await operation.ProcessAsync(
            transaction,
            Input(0, "ip", Start.AddMinutes(1), "alice"),
            Start.AddMinutes(5).AddSeconds(30));
        var rejected = await operation.ProcessAsync(
            transaction,
            Input(0, "other", Start.AddMinutes(1), "bob"),
            Start.AddMinutes(6));

        Assert.AreEqual(LateEventDisposition.LateAccepted, accepted.LateEvent.Disposition);
        Assert.IsNotNull(accepted.Change);
        Assert.AreEqual(LateEventDisposition.TooLate, rejected.LateEvent.Disposition);
        Assert.IsNull(rejected.Change);
        Assert.AreEqual(LateEventAction.SideOutput, rejected.LateEvent.Action);
    }

    [TestMethod]
    public async Task Finalize_EmitsOnceAtBoundaryAndPreventsReopening()
    {
        var store = new InMemoryQueryStateStore();
        var operation = Operator();
        var window = new WindowInterval(Start, Start.AddMinutes(5));
        await using var transaction = await store.BeginTransactionAsync(Domain, Range(0, 0));
        await operation.ProcessAsync(transaction, Input(0, "ip", Start, "alice"), null);

        Assert.IsNull(await operation.FinalizeAsync(transaction, 0, "ip", window, Start.AddMinutes(6).AddTicks(-1)));
        var change = await operation.FinalizeAsync(transaction, 0, "ip", window, Start.AddMinutes(6));
        Assert.IsNotNull(change);
        Assert.AreEqual(QueryChangeKind.Finalize, change.Kind);
        Assert.IsNull(change.Value);
        Assert.IsNull(await operation.FinalizeAsync(transaction, 0, "ip", window, Start.AddMinutes(7)));
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await operation.ProcessAsync(transaction, Input(0, "ip", Start, "bob"), null));
    }

    [TestMethod]
    public async Task CommittedRangeReplay_CannotExecuteOperatorAgain()
    {
        var store = new InMemoryQueryStateStore();
        var range = Range(0, 0);
        await using (var transaction = await store.BeginTransactionAsync(Domain, range))
        {
            await Operator().ProcessAsync(transaction, Input(0, "ip", Start, "alice"), null);
            await transaction.CommitAsync(range.EndOffset);
        }

        await using var replay = await store.BeginTransactionAsync(Domain, range);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await Operator().ProcessAsync(replay, Input(0, "ip", Start, "alice"), null));
    }

    [TestMethod]
    public async Task TransactionRollback_DiscardsAggregateUpdate()
    {
        var store = new InMemoryQueryStateStore();
        var range = Range(0, 0);
        await using (var transaction = await store.BeginTransactionAsync(Domain, range))
        {
            await Operator().ProcessAsync(transaction, Input(0, "ip", Start, "alice"), null);
        }

        await using var retry = await store.BeginTransactionAsync(Domain, range);
        var result = await Operator().ProcessAsync(retry, Input(0, "ip", Start, "alice"), null);
        Assert.AreEqual(QueryChangeKind.Upsert, result.Change!.Kind);
        Assert.AreEqual((1L, 1), AggregateResultValueCodec.Deserialize(result.Change.Value!.Value.Span));
    }

    [TestMethod]
    public void AggregateValueCodec_RejectsUnknownAndMalformedPayloads()
    {
        var payload = AggregateResultValueCodec.Serialize(1, 1);
        payload[7] = 2;
        Assert.ThrowsExactly<InvalidDataException>(() => AggregateResultValueCodec.Deserialize(payload));
        Assert.ThrowsExactly<InvalidDataException>(() => AggregateResultValueCodec.Deserialize(new byte[19]));
    }

    private static TumblingWindowAggregateOperator Operator() => new(
        "window-aggregate",
        "detections",
        "query-results",
        new TumblingWindowAssigner(TimeSpan.FromMinutes(5)),
        new LateEventPolicy(TimeSpan.FromMinutes(1), LateEventAction.SideOutput),
        new ExactDistinctPolicy(100, 4096));

    private static TumblingAggregateInput Input(
        int partition,
        string key,
        DateTimeOffset timestamp,
        string distinct) => new(partition, key, timestamp, Encoding.UTF8.GetBytes(distinct));

    private static SourceRange Range(long start, long end) => new("events", 0, start, end);

    private static async Task<IReadOnlyList<OutputIntent>> ReadIntents(IQueryStateStore store)
    {
        var intents = new List<OutputIntent>();
        await foreach (var intent in store.ReadPendingOutputIntentsAsync(Domain)) intents.Add(intent);
        return intents;
    }
}
