using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;
using DeltaZulu.LocalStream.Query.Results;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class InMemoryQueryStateStoreTests
{
    private static readonly StateDomainId Domain = new("detections", 1, "partition-0");
    private static readonly SourceRange Range = new("authentication", 0, 10, 19);
    private static readonly StateKey Key = new("aggregate-1", 0, "192.0.2.1");

    [TestMethod]
    public async Task Commit_AtomicallyPublishesStateCheckpointWatermarkAndOutputIntent()
    {
        var store = new InMemoryQueryStateStore();
        var watermark = new WatermarkState(DateTimeOffset.UnixEpoch, []);
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range, TestContext.CancellationToken))
        {
            transaction.PutOperatorState(Key, new byte[] { 1, 2, 3 });
            transaction.SetWatermark(watermark);
            transaction.AddOutputIntent(new OutputIntent(ChangeId("change-1"), "results", new byte[] { 4, 5 }));
            var checkpoint = await transaction.CommitAsync(19, TestContext.CancellationToken);

            Assert.AreEqual(1, checkpoint.Generation);
            Assert.AreEqual(watermark, checkpoint.Watermark);
            Assert.AreEqual(19, checkpoint.CursorOffset);
        }

        var restored = await store.GetCheckpointAsync(Domain, TestContext.CancellationToken);
        Assert.IsNotNull(restored);
        var intents = await ReadIntents(store);
        Assert.HasCount(1, intents);
        Assert.AreSequenceEqual(new byte[] { 4, 5 }, intents[0].Payload.ToArray());

        await using var next = await store.BeginTransactionAsync(Domain, new SourceRange("authentication", 0, 20, 20), TestContext.CancellationToken);
        var state = await next.GetOperatorStateAsync(Key, TestContext.CancellationToken);
        Assert.AreSequenceEqual(new byte[] { 1, 2, 3 }, state!.Value.ToArray());
    }

    [TestMethod]
    public async Task DisposeWithoutCommit_DiscardsEveryStagedChange()
    {
        var store = new InMemoryQueryStateStore();
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range, TestContext.CancellationToken))
        {
            transaction.PutOperatorState(Key, new byte[] { 1 });
            transaction.AddOutputIntent(new OutputIntent(ChangeId("change-1"), "results", new byte[] { 2 }));
        }

        Assert.IsNull(await store.GetCheckpointAsync(Domain, TestContext.CancellationToken));
        Assert.HasCount(0, await ReadIntents(store));
        await using var retry = await store.BeginTransactionAsync(Domain, Range, TestContext.CancellationToken);
        Assert.IsNull(await retry.GetOperatorStateAsync(Key, TestContext.CancellationToken));
    }

    [TestMethod]
    public async Task ReplayOfCommittedRange_IsReadOnlyAndDoesNotAdvanceGeneration()
    {
        var store = new InMemoryQueryStateStore();
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range, TestContext.CancellationToken))
        {
            transaction.PutOperatorState(Key, new byte[] { 1 });
            await transaction.CommitAsync(19, TestContext.CancellationToken);
        }

        await using var replay = await store.BeginTransactionAsync(Domain, Range, TestContext.CancellationToken);
        Assert.IsTrue(replay.IsAlreadyCommitted);
        Assert.ThrowsExactly<InvalidOperationException>(() => replay.PutOperatorState(Key, new byte[] { 2 }));
        var checkpoint = await replay.CommitAsync(19, TestContext.CancellationToken);
        Assert.AreEqual(1, checkpoint.Generation);
    }

    public async Task Domain_AllowsOnlyOneActiveWriter()
    {
        var store = new InMemoryQueryStateStore();
        await using var first = await store.BeginTransactionAsync(Domain, Range, TestContext.CancellationToken);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await store.BeginTransactionAsync(Domain, new SourceRange("authentication", 0, 20, 29), TestContext.CancellationToken));
    }

    [TestMethod]
    public async Task Commit_RequiresCursorAtExactRangeEnd()
    {
        var store = new InMemoryQueryStateStore();
        await using var transaction = await store.BeginTransactionAsync(Domain, Range, TestContext.CancellationToken);

        await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(async () =>
            await transaction.CommitAsync(18, TestContext.CancellationToken));
    }

    [TestMethod]
    public async Task DeliveredOutputIntent_IsRemovedIdempotently()
    {
        var store = new InMemoryQueryStateStore();
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range, TestContext.CancellationToken))
        {
            transaction.AddOutputIntent(new OutputIntent(ChangeId("change-2"), "results", new byte[] { 2 }));
            transaction.AddOutputIntent(new OutputIntent(ChangeId("change-1"), "results", new byte[] { 1 }));
            await transaction.CommitAsync(19, TestContext.CancellationToken);
        }

        var pending = await ReadIntents(store);
        var expected = new[] { ChangeId("change-1"), ChangeId("change-2") }
            .OrderBy(identity => identity.Value, StringComparer.Ordinal)
            .ToArray();
        Assert.AreSequenceEqual(expected, pending.Select(intent => intent.ResultChangeId).ToArray());
        Assert.IsTrue(await store.MarkOutputIntentDeliveredAsync(Domain, ChangeId("change-1"), TestContext.CancellationToken));
        Assert.IsFalse(await store.MarkOutputIntentDeliveredAsync(Domain, ChangeId("change-1"), TestContext.CancellationToken));
        Assert.AreSequenceEqual(
            new[] { ChangeId("change-2") }, (await ReadIntents(store)).Select(intent => intent.ResultChangeId).ToArray());
    }

    private static async Task<IReadOnlyList<OutputIntent>> ReadIntents(IQueryStateStore store)
    {
        var intents = new List<OutputIntent>();
        await foreach (var intent in store.ReadPendingOutputIntentsAsync(Domain))
        {
            intents.Add(intent);
        }

        return intents;
    }

    private static ResultChangeId ChangeId(string key) => ResultIdentityBuilder.Build(new ResultIdentity(
        Domain.QueryId,
        Domain.Revision,
        "results",
        "aggregate-1",
        key,
        null,
        1,
        Range));

    public TestContext TestContext { get; set; }
}
