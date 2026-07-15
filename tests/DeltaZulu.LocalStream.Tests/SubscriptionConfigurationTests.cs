namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class SubscriptionConfigurationTests
{
    public TestContext TestContext { get; set; } = null!;

    private static LocalStreamOptions OptionsWithSubscription(
        string dir,
        string subscriptionId,
        StartPosition startPosition,
        bool required = false)
    {
        var options = TestHost.Options(dir);
        options.Subscriptions.Add(new SubscriptionOptions
        {
            Id = subscriptionId,
            Topic = "agent.output",
            Required = required,
            StartPosition = startPosition,
        });
        return options;
    }

    [TestMethod]
    public async Task ConfiguredSubscription_StartPositionEarliest_ReadsAllRetainedRecords()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            for (var i = 0; i < 5; i++)
            {
                await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
            }
        }

        await using (var host = await TestHost.StartAsync(
            OptionsWithSubscription(dir, "archive", StartPosition.Earliest)))
        {
            var consumer = host.CreateConsumer<TestEvent>("archive");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");
            Assert.AreEqual(5, records.Count);
        }
    }

    [TestMethod]
    public async Task ConfiguredSubscription_StartPositionLatest_SkipsRecordsFromBeforeStart()
    {
        var dir = TestHost.NewStorageDir(TestContext);

        await using (var host = await TestHost.StartAsync(TestHost.Options(dir)))
        {
            var producer = host.CreateProducer<TestEvent>();
            for (var i = 0; i < 5; i++)
            {
                await producer.AppendAsync("agent.output", new TestEvent("s", $"old-{i}"));
            }
        }

        await using (var host = await TestHost.StartAsync(
            OptionsWithSubscription(dir, "logcluster", StartPosition.Latest)))
        {
            var producer = host.CreateProducer<TestEvent>();
            await producer.AppendAsync("agent.output", new TestEvent("s", "new-0"));
            await producer.AppendAsync("agent.output", new TestEvent("s", "new-1"));

            var consumer = host.CreateConsumer<TestEvent>("logcluster");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");

            Assert.AreEqual(2, records.Count);
            Assert.AreEqual("new-0", records[0].Payload.Message);
            Assert.AreEqual("new-1", records[1].Payload.Message);
        }
    }

    [TestMethod]
    public async Task ConfiguredSubscription_LatestStart_DoesNotResetExistingCheckpoint()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = OptionsWithSubscription(dir, "silver", StartPosition.Latest);

        await using (var host = await TestHost.StartAsync(options))
        {
            var producer = host.CreateProducer<TestEvent>();
            for (var i = 0; i < 4; i++)
            {
                await producer.AppendAsync("agent.output", new TestEvent("s", $"m{i}"));
            }

            var consumer = host.CreateConsumer<TestEvent>("silver");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");
            Assert.AreEqual(4, records.Count);
            await consumer.CommitAsync(records[1].Position);
        }

        // Restart: startPosition must only apply to brand-new subscriptions,
        // never to one that already has a checkpoint.
        await using (var host = await TestHost.StartAsync(
            OptionsWithSubscription(dir, "silver", StartPosition.Latest)))
        {
            var consumer = host.CreateConsumer<TestEvent>("silver");
            var records = await TestHost.ReadAllAsync(consumer, "agent.output");

            Assert.AreEqual(2, records.Count);
            Assert.AreEqual(2, records[0].Offset);
        }
    }

    [TestMethod]
    public async Task ConfiguredSubscription_UnknownTopic_FailsStartup()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        var options = TestHost.Options(dir);
        options.Subscriptions.Add(new SubscriptionOptions
        {
            Id = "archive",
            Topic = "no.such.topic",
        });

        var host = new LocalStreamHost(options);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => host.StartAsync());
    }

    [TestMethod]
    public async Task SubscriptionMetadata_IsWrittenAtStartup()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(
            OptionsWithSubscription(dir, "archive", StartPosition.Earliest, required: true));

        var metadataPath = Path.Combine(dir, "metadata", "subscriptions.json");
        Assert.IsTrue(File.Exists(metadataPath));
        StringAssert.Contains(await File.ReadAllTextAsync(metadataPath), "archive");
    }
}
