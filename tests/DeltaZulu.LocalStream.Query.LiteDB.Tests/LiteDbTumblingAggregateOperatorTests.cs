using System.Text;
using DeltaZulu.LocalStream.Query.LiteDB;
using DeltaZulu.LocalStream.Query.Operators;
using DeltaZulu.LocalStream.Query.State;
using DeltaZulu.LocalStream.Query.Time;
using DeltaZulu.LocalStream.Query.Results;

namespace DeltaZulu.LocalStream.Query.LiteDB.Tests;

[TestClass]
public sealed class LiteDbTumblingAggregateOperatorTests
{
    private static readonly StateDomainId Domain = new("failed-auth", 1, "p0");
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task AggregateStateAndLogicalVersionSurviveDatabaseReopen()
    {
        var path = DatabasePath();
        using (var store = new LiteDbStateStore(path))
        await using (var transaction = await store.BeginTransactionAsync(
            Domain,
            new SourceRange("events", 0, 0, 0)))
        {
            var result = await Operator().ProcessAsync(transaction, Input("alice"), null);
            Assert.AreEqual(1, result.Change!.Version);
            await transaction.CommitAsync(0);
        }

        using var reopened = new LiteDbStateStore(path);
        await using var next = await reopened.BeginTransactionAsync(
            Domain,
            new SourceRange("events", 0, 1, 1));
        var correction = await Operator().ProcessAsync(next, Input("bob"), null);
        Assert.AreEqual(2, correction.Change!.Version);
        Assert.AreEqual(
            (2L, 2),
            AggregateResultValueCodec.Deserialize(correction.Change.Value!.Value.Span));
        await next.CommitAsync(1);
        var intents = new List<OutputIntent>();
        await foreach (var intent in reopened.ReadPendingOutputIntentsAsync(Domain)) intents.Add(intent);
        Assert.HasCount(2, intents);
        Assert.IsTrue(intents.All(intent => QueryChangeCodec.Deserialize(intent.Payload.Span).ChangeId == intent.ResultChangeId));
    }

    [TestMethod]
    public async Task RolledBackAggregateDoesNotAppearAfterReopen()
    {
        var path = DatabasePath();
        using (var store = new LiteDbStateStore(path))
        await using (var transaction = await store.BeginTransactionAsync(
            Domain,
            new SourceRange("events", 0, 0, 0)))
        {
            await Operator().ProcessAsync(transaction, Input("alice"), null);
        }

        using var reopened = new LiteDbStateStore(path);
        await using var retry = await reopened.BeginTransactionAsync(
            Domain,
            new SourceRange("events", 0, 0, 0));
        var result = await Operator().ProcessAsync(retry, Input("alice"), null);
        Assert.AreEqual(1, result.Change!.Version);
        Assert.AreEqual(
            (1L, 1),
            AggregateResultValueCodec.Deserialize(result.Change.Value!.Value.Span));
    }

    private static TumblingWindowAggregateOperator Operator() => new(
        "window-aggregate",
        "detections",
        "query-results",
        new TumblingWindowAssigner(TimeSpan.FromMinutes(5)),
        new LateEventPolicy(TimeSpan.FromMinutes(1), LateEventAction.SideOutput),
        new ExactDistinctPolicy(100, 4096));

    private static TumblingAggregateInput Input(string user) => new(
        0,
        "192.0.2.1",
        DateTimeOffset.UnixEpoch,
        Encoding.UTF8.GetBytes(user));

    private string DatabasePath()
    {
        var directory = Path.Combine(TestContext.TestRunDirectory!, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, "aggregate.db");
    }
}
