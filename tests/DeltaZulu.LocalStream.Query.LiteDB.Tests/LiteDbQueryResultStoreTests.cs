using DeltaZulu.LocalStream.Query.LiteDB;
using DeltaZulu.LocalStream.Query.Results;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.LiteDB.Tests;

[TestClass]
public sealed class LiteDbQueryResultStoreTests
{
    private static readonly StateDomainId Domain = new("query", 1, "p0");
    private static readonly SourceRange Range = new("events", 0, 0, 9);
    private static readonly ResultKey Key = new("key", new WindowInterval(
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch.AddMinutes(5)));
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task AppliedChangeAndDeduplicationSurviveReopen()
    {
        var path = DatabasePath();
        var change = Change(QueryChangeKind.Upsert, 1, new byte[] { 1 });
        using (var store = new LiteDbQueryResultStore(path))
        {
            Assert.AreEqual(MaterializationOutcome.Applied, await store.ApplyAsync(Domain, change));
        }

        using var reopened = new LiteDbQueryResultStore(path);
        Assert.AreEqual(MaterializationOutcome.Duplicate, await reopened.ApplyAsync(Domain, change));
        var result = await reopened.GetAsync(Domain, Key);
        Assert.IsNotNull(result);
        CollectionAssert.AreEqual(new byte[] { 1 }, result.Value!.Value.ToArray());
    }

    [TestMethod]
    public async Task TombstoneAndFinalizationSurviveReopen()
    {
        var path = DatabasePath();
        using (var store = new LiteDbQueryResultStore(path))
        {
            await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 1, new byte[] { 1 }));
            await store.ApplyAsync(Domain, Change(QueryChangeKind.Delete, 2));
            await store.ApplyAsync(Domain, Change(QueryChangeKind.Finalize, 3));
        }

        using var reopened = new LiteDbQueryResultStore(path);
        var result = await reopened.GetAsync(Domain, Key);
        Assert.IsNotNull(result);
        Assert.IsTrue(result.IsDeleted);
        Assert.IsTrue(result.IsFinalized);
        Assert.IsNull(result.Value);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await reopened.ApplyAsync(Domain, Change(QueryChangeKind.Correction, 4, new byte[] { 4 })));
    }

    [TestMethod]
    public async Task StaleChangeIsRememberedAndCannotResurrectResult()
    {
        using var store = new LiteDbQueryResultStore(DatabasePath());
        await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 2, new byte[] { 2 }));
        var stale = Change(
            QueryChangeKind.Upsert,
            1,
            new byte[] { 1 },
            new SourceRange("events", 0, 10, 19));

        Assert.AreEqual(MaterializationOutcome.Stale, await store.ApplyAsync(Domain, stale));
        Assert.AreEqual(MaterializationOutcome.Duplicate, await store.ApplyAsync(Domain, stale));
        Assert.AreEqual(2, (await store.GetAsync(Domain, Key))!.Version);
    }

    [TestMethod]
    public async Task ReadAllIsDeterministicallyOrderedAfterReopen()
    {
        var path = DatabasePath();
        using (var store = new LiteDbQueryResultStore(path))
        {
            await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 1, new byte[] { 1 }, key: new ResultKey("z")));
            await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 1, new byte[] { 2 }, key: new ResultKey("a")));
        }

        using var reopened = new LiteDbQueryResultStore(path);
        var rows = new List<MaterializedResult>();
        await foreach (var row in reopened.ReadAllAsync(Domain)) rows.Add(row);
        CollectionAssert.AreEqual(new[] { "a", "z" }, rows.Select(row => row.Key.CanonicalKey).ToArray());
    }

    [TestMethod]
    public async Task PendingIntent_ReplayedAfterSinkCommitBeforeAcknowledgement()
    {
        var statePath = DatabasePath();
        var resultPath = DatabasePath();
        var change = Change(QueryChangeKind.Upsert, 1, new byte[] { 1, 2 });
        var payload = QueryChangeCodec.Serialize(change);

        using (var state = new LiteDbStateStore(statePath))
        {
            await using var transaction = await state.BeginTransactionAsync(Domain, Range);
            transaction.AddOutputIntent(new OutputIntent(change.ChangeId, "results", payload));
            await transaction.CommitAsync(Range.EndOffset);
        }

        using (var state = new LiteDbStateStore(statePath))
        using (var results = new LiteDbQueryResultStore(resultPath))
        {
            await foreach (var intent in state.ReadPendingOutputIntentsAsync(Domain))
            {
                CollectionAssert.AreEqual(payload, intent.Payload.ToArray());
                Assert.AreEqual(MaterializationOutcome.Applied,
                    await results.ApplyAsync(Domain, QueryChangeCodec.Deserialize(intent.Payload.Span)));
            }
            // Simulate interruption after the sink commit, before delivery confirmation.
        }

        using (var state = new LiteDbStateStore(statePath))
        using (var results = new LiteDbQueryResultStore(resultPath))
        {
            var count = 0;
            await foreach (var intent in state.ReadPendingOutputIntentsAsync(Domain))
            {
                count++;
                Assert.AreEqual(change.ChangeId, intent.ResultChangeId);
                Assert.AreEqual(MaterializationOutcome.Duplicate,
                    await results.ApplyAsync(Domain, QueryChangeCodec.Deserialize(intent.Payload.Span)));
                Assert.IsTrue(await state.MarkOutputIntentDeliveredAsync(Domain, intent.ResultChangeId));
            }
            Assert.AreEqual(1, count);
            var result = await results.GetAsync(Domain, Key);
            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(new byte[] { 1, 2 }, result.Value!.Value.ToArray());
            Assert.AreEqual(1L, result.Version);
        }

        using var reopened = new LiteDbStateStore(statePath);
        await foreach (var intent in reopened.ReadPendingOutputIntentsAsync(Domain))
        {
            Assert.Fail($"Delivered intent {intent.ResultChangeId} remained pending.");
        }
        Assert.AreEqual(Range.EndOffset, (await reopened.GetCheckpointAsync(Domain))!.CursorOffset);
    }

    private static QueryChange Change(
        QueryChangeKind kind,
        long version,
        byte[]? value = null,
        SourceRange? causality = null,
        ResultKey? key = null)
    {
        causality ??= Range;
        key ??= Key;
        var id = ResultIdentityBuilder.Build(new ResultIdentity(
            Domain.QueryId, Domain.Revision, "results", "aggregate", key.CanonicalKey,
            key.Window, version, causality));
        ReadOnlyMemory<byte>? payload = value is null
            ? default(ReadOnlyMemory<byte>?)
            : new ReadOnlyMemory<byte>(value);
        return new QueryChange(id, kind, key, version, payload, causality);
    }

    private string DatabasePath()
    {
        var directory = Path.Combine(TestContext.TestRunDirectory!, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, "results.db");
    }
}
