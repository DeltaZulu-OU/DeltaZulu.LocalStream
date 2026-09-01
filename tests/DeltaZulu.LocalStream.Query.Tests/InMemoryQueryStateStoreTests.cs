using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

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
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range))
        {
            transaction.PutOperatorState(Key, new byte[] { 1, 2, 3 });
            transaction.SetWatermark(watermark);
            transaction.AddOutputIntent(new OutputIntent("change-1", "results", new byte[] { 4, 5 }));
            var checkpoint = await transaction.CommitAsync(19);

            Assert.AreEqual(1, checkpoint.Generation);
            Assert.AreEqual(watermark, checkpoint.Watermark);
            Assert.AreEqual(19, checkpoint.CursorOffset);
        }

        var restored = await store.GetCheckpointAsync(Domain);
        Assert.IsNotNull(restored);
        var intents = await ReadIntents(store);
        Assert.HasCount(1, intents);
        CollectionAssert.AreEqual(new byte[] { 4, 5 }, intents[0].Payload.ToArray());

        await using var next = await store.BeginTransactionAsync(Domain, new SourceRange("authentication", 0, 20, 20));
        var state = await next.GetOperatorStateAsync(Key);
        CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, state!.Value.ToArray());
    }

    [TestMethod]
    public async Task DisposeWithoutCommit_DiscardsEveryStagedChange()
    {
        var store = new InMemoryQueryStateStore();
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range))
        {
            transaction.PutOperatorState(Key, new byte[] { 1 });
            transaction.AddOutputIntent(new OutputIntent("change-1", "results", new byte[] { 2 }));
        }

        Assert.IsNull(await store.GetCheckpointAsync(Domain));
        Assert.HasCount(0, await ReadIntents(store));
        await using var retry = await store.BeginTransactionAsync(Domain, Range);
        Assert.IsNull(await retry.GetOperatorStateAsync(Key));
    }

    [TestMethod]
    public async Task ReplayOfCommittedRange_IsReadOnlyAndDoesNotAdvanceGeneration()
    {
        var store = new InMemoryQueryStateStore();
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range))
        {
            transaction.PutOperatorState(Key, new byte[] { 1 });
            await transaction.CommitAsync(19);
        }

        await using var replay = await store.BeginTransactionAsync(Domain, Range);
        Assert.IsTrue(replay.IsAlreadyCommitted);
        Assert.ThrowsExactly<InvalidOperationException>(() => replay.PutOperatorState(Key, new byte[] { 2 }));
        var checkpoint = await replay.CommitAsync(19);
        Assert.AreEqual(1, checkpoint.Generation);
    }

    [TestMethod]
    public async Task Domain_AllowsOnlyOneActiveWriter()
    {
        var store = new InMemoryQueryStateStore();
        await using var first = await store.BeginTransactionAsync(Domain, Range);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await store.BeginTransactionAsync(Domain, new SourceRange("authentication", 0, 20, 29)));
    }

    [TestMethod]
    public async Task Commit_RequiresCursorAtExactRangeEnd()
    {
        var store = new InMemoryQueryStateStore();
        await using var transaction = await store.BeginTransactionAsync(Domain, Range);

        await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(async () =>
            await transaction.CommitAsync(18));
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
}
