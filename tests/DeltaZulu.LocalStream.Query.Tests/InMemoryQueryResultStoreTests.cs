using DeltaZulu.LocalStream.Query.Results;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;

namespace DeltaZulu.LocalStream.Query.Tests;

[TestClass]
public sealed class InMemoryQueryResultStoreTests
{
    private static readonly StateDomainId Domain = new("query", 1, "p0");
    private static readonly SourceRange Range = new("events", 0, 0, 9);
    private static readonly ResultKey Key = new("key", new WindowInterval(
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch.AddMinutes(5)));

    [TestMethod]
    public async Task Retry_IsDuplicateAndCorrectionReplacesCurrentValue()
    {
        var store = new InMemoryQueryResultStore();
        var upsert = Change(QueryChangeKind.Upsert, 1, new byte[] { 1 });

        Assert.AreEqual(MaterializationOutcome.Applied, await store.ApplyAsync(Domain, upsert));
        Assert.AreEqual(MaterializationOutcome.Duplicate, await store.ApplyAsync(Domain, upsert));
        Assert.AreEqual(
            MaterializationOutcome.Applied,
            await store.ApplyAsync(Domain, Change(QueryChangeKind.Correction, 2, new byte[] { 2 })));

        var result = await store.GetAsync(Domain, Key);
        Assert.IsNotNull(result);
        Assert.AreEqual(2, result.Version);
        CollectionAssert.AreEqual(new byte[] { 2 }, result.Value!.Value.ToArray());
    }

    [TestMethod]
    public async Task StaleChange_CannotOverwriteNewerVersion()
    {
        var store = new InMemoryQueryResultStore();
        await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 2, new byte[] { 2 }));

        Assert.AreEqual(
            MaterializationOutcome.Stale,
            await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 1, new byte[] { 1 })));
        Assert.AreEqual(2, (await store.GetAsync(Domain, Key))!.Version);
    }

    [TestMethod]
    public async Task DeleteLeavesVersionedTombstoneThatBlocksOlderReplay()
    {
        var store = new InMemoryQueryResultStore();
        await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 1, new byte[] { 1 }));
        await store.ApplyAsync(Domain, Change(QueryChangeKind.Delete, 2));

        var result = await store.GetAsync(Domain, Key);
        Assert.IsNotNull(result);
        Assert.IsTrue(result.IsDeleted);
        Assert.IsNull(result.Value);
        Assert.AreEqual(MaterializationOutcome.Stale,
            await store.ApplyAsync(Domain, Change(
                QueryChangeKind.Upsert,
                1,
                new byte[] { 1 },
                causality: new SourceRange("events", 0, 10, 19))));
    }

    [TestMethod]
    public async Task FinalizedResultCannotBeReopened()
    {
        var store = new InMemoryQueryResultStore();
        await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 1, new byte[] { 1 }));
        await store.ApplyAsync(Domain, Change(QueryChangeKind.Finalize, 2));

        Assert.IsTrue((await store.GetAsync(Domain, Key))!.IsFinalized);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await store.ApplyAsync(Domain, Change(QueryChangeKind.Correction, 3, new byte[] { 3 })));
    }

    [TestMethod]
    public async Task ReadAll_IsDeterministicallyOrderedAndReturnsCopies()
    {
        var store = new InMemoryQueryResultStore();
        var payload = new byte[] { 1 };
        await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 1, payload, new ResultKey("z")));
        payload[0] = 9;
        await store.ApplyAsync(Domain, Change(QueryChangeKind.Upsert, 1, new byte[] { 2 }, new ResultKey("a")));

        var rows = await ReadAll(store);
        CollectionAssert.AreEqual(new[] { "a", "z" }, rows.Select(row => row.Key.CanonicalKey).ToArray());
        CollectionAssert.AreEqual(new byte[] { 1 }, rows[1].Value!.Value.ToArray());
    }

    private static QueryChange Change(
        QueryChangeKind kind,
        long version,
        byte[]? value = null,
        ResultKey? key = null,
        SourceRange? causality = null)
    {
        key ??= Key;
        causality ??= Range;
        var identity = ResultIdentityBuilder.Build(new ResultIdentity(
            Domain.QueryId, Domain.Revision, "results", "aggregate", key.CanonicalKey,
            key.Window, version, causality));
        ReadOnlyMemory<byte>? payload = value is null
            ? default(ReadOnlyMemory<byte>?)
            : new ReadOnlyMemory<byte>(value);
        return new QueryChange(identity, kind, key, version, payload, causality);
    }

    private static async Task<IReadOnlyList<MaterializedResult>> ReadAll(IQueryResultStore store)
    {
        var rows = new List<MaterializedResult>();
        await foreach (var row in store.ReadAllAsync(Domain)) rows.Add(row);
        return rows;
    }
}
