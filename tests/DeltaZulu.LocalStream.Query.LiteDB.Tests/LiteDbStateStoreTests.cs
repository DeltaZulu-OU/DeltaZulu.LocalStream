using DeltaZulu.LocalStream.Query.LiteDB;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;
using DeltaZulu.LocalStream.Query.Results;

namespace DeltaZulu.LocalStream.Query.LiteDB.Tests;

[TestClass]
public sealed class LiteDbStateStoreTests
{
    private static readonly StateDomainId Domain = new("query", 1, "p0");
    private static readonly SourceRange Range = new("events", 0, 0, 4);
    private static readonly StateKey Key = new("aggregate", 0, "key");
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task Commit_SurvivesDatabaseReopen()
    {
        var path = DatabasePath();
        using (var store = new LiteDbStateStore(path))
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range))
        {
            transaction.PutOperatorState(Key, new byte[] { 1, 2 });
            transaction.SetWatermark(new WatermarkState(DateTimeOffset.UnixEpoch, []));
            transaction.AddOutputIntent(new OutputIntent(ChangeId(), "target", new byte[] { 3 }));
            await transaction.CommitAsync(4);
        }

        using var reopened = new LiteDbStateStore(path);
        var checkpoint = await reopened.GetCheckpointAsync(Domain);
        Assert.IsNotNull(checkpoint);
        Assert.AreEqual(4, checkpoint.CursorOffset);
        Assert.IsNotNull(checkpoint.Watermark);
        var intents = await ReadIntents(reopened);
        Assert.HasCount(1, intents);
        CollectionAssert.AreEqual(new byte[] { 3 }, intents[0].Payload.ToArray());
        await using var next = await reopened.BeginTransactionAsync(Domain, new SourceRange("events", 0, 5, 5));
        var state = await next.GetOperatorStateAsync(Key);
        CollectionAssert.AreEqual(new byte[] { 1, 2 }, state!.Value.ToArray());
    }

    [TestMethod]
    public async Task DisposeWithoutCommit_RollsBackAllDocuments()
    {
        var path = DatabasePath();
        using var store = new LiteDbStateStore(path);
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range))
        {
            transaction.PutOperatorState(Key, new byte[] { 1 });
            transaction.AddOutputIntent(new OutputIntent(ChangeId(), "target", new byte[] { 2 }));
        }

        Assert.IsNull(await store.GetCheckpointAsync(Domain));
        Assert.HasCount(0, await ReadIntents(store));
        await using var retry = await store.BeginTransactionAsync(Domain, Range);
        Assert.IsNull(await retry.GetOperatorStateAsync(Key));
    }

    [TestMethod]
    public async Task Replay_IsReadOnlyAndKeepsCheckpointGeneration()
    {
        using var store = new LiteDbStateStore(DatabasePath());
        await using (var transaction = await store.BeginTransactionAsync(Domain, Range))
        {
            await transaction.CommitAsync(4);
        }

        await using var replay = await store.BeginTransactionAsync(Domain, Range);
        Assert.IsTrue(replay.IsAlreadyCommitted);
        Assert.ThrowsExactly<InvalidOperationException>(() => replay.PutOperatorState(Key, new byte[] { 1 }));
        Assert.AreEqual(1, (await replay.CommitAsync(4)).Generation);
    }

    [TestMethod]
    public async Task DeliveredOutputIntent_RemainsRemovedAfterReopen()
    {
        var path = DatabasePath();
        using (var store = new LiteDbStateStore(path))
        {
            await using (var transaction = await store.BeginTransactionAsync(Domain, Range))
            {
                transaction.AddOutputIntent(new OutputIntent(ChangeId(), "target", new byte[] { 1 }));
                await transaction.CommitAsync(4);
            }

            Assert.IsTrue(await store.MarkOutputIntentDeliveredAsync(Domain, ChangeId()));
            Assert.IsFalse(await store.MarkOutputIntentDeliveredAsync(Domain, ChangeId()));
        }

        using var reopened = new LiteDbStateStore(path);
        Assert.HasCount(0, await ReadIntents(reopened));
    }

    private string DatabasePath()
    {
        var directory = Path.Combine(TestContext.TestRunDirectory!, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, "state.db");
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

    private static ResultChangeId ChangeId() => ResultIdentityBuilder.Build(new ResultIdentity(
        Domain.QueryId,
        Domain.Revision,
        "target",
        "aggregate",
        "key",
        null,
        1,
        Range));
}
