namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class ProcessorTests
{
    public TestContext TestContext { get; set; } = null!;

    private static LocalStreamOptions ProcessorOptions(string dir)
    {
        var options = new LocalStreamOptions { StoragePath = dir };
        options.Topics.Add(new TopicOptions { Name = "agent.output" });
        options.Topics.Add(new TopicOptions { Name = "logcluster.samples" });
        return options;
    }

    /// <summary>Forwards parse failures to the sample topic, drops the rest.</summary>
    private sealed class SamplerProcessor : ILocalStreamProcessor<TestEvent, TestEvent>
    {
        public async ValueTask ProcessAsync(
            StreamRecord<TestEvent> input,
            ILocalStreamProducer<TestEvent> output,
            ProcessorContext context,
            CancellationToken cancellationToken = default)
        {
            if (input.Payload.Message.Contains("parse_failed", StringComparison.Ordinal))
            {
                // Deterministic output ID so a crash-induced rerun overwrites
                // rather than duplicates downstream.
                await output.AppendAsync(
                    "logcluster.samples",
                    input.Payload,
                    new AppendOptions { EventId = $"sample-{input.EventId}" },
                    cancellationToken);
            }
        }
    }

    private sealed class FailAfterOutputProcessor : ILocalStreamProcessor<TestEvent, TestEvent>
    {
        public async ValueTask ProcessAsync(
            StreamRecord<TestEvent> input,
            ILocalStreamProducer<TestEvent> output,
            ProcessorContext context,
            CancellationToken cancellationToken = default)
        {
            await output.AppendAsync("logcluster.samples", input.Payload, null, cancellationToken);
            throw new InvalidOperationException("sink exploded after output append");
        }
    }

    private static async Task<int> AppendInputAsync(LocalStreamHost host, params string[] messages)
    {
        var producer = host.CreateProducer<TestEvent>();
        foreach (var message in messages)
        {
            await producer.AppendAsync("agent.output", new TestEvent("s", message));
        }

        return messages.Length;
    }

    [TestMethod]
    public async Task Processor_TransformsInput_AndCommitsAfterProcessing()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(ProcessorOptions(dir));
        await AppendInputAsync(host, "ok-1", "parse_failed-1", "ok-2", "parse_failed-2");

        var processed = await host.RunProcessorOnceAsync(
            "logcluster-sampler", "agent.output", new SamplerProcessor());

        Assert.AreEqual(4, processed);

        var samples = await TestHost.ReadAllAsync(
            host.CreateConsumer<TestEvent>("debug-local"), "logcluster.samples");
        Assert.AreEqual(2, samples.Count);
        Assert.IsTrue(samples.All(s => s.Payload.Message.Contains("parse_failed", StringComparison.Ordinal)));

        // Input fully committed under the processor's own subscription.
        Assert.AreEqual(
            0,
            host.GetSubscriptionMetrics("processor.logcluster-sampler", "agent.output").TotalLagRecords);

        // Idempotent rerun: nothing new to process.
        Assert.AreEqual(
            0,
            await host.RunProcessorOnceAsync("logcluster-sampler", "agent.output", new SamplerProcessor()));
    }

    [TestMethod]
    public async Task FailingProcessor_WritesOutputButDoesNotCommitInput()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(ProcessorOptions(dir));
        await AppendInputAsync(host, "m0");

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => host.RunProcessorOnceAsync(
                "logcluster-sampler", "agent.output", new FailAfterOutputProcessor()));

        // Output-first, commit-after: the output record exists even though the
        // input offset was never committed. Duplicates after crash are the
        // documented at-least-once behavior; sinks deduplicate by EventId.
        var samples = await TestHost.ReadAllAsync(
            host.CreateConsumer<TestEvent>("debug-local"), "logcluster.samples");
        Assert.AreEqual(1, samples.Count);

        Assert.AreEqual(
            1,
            host.GetSubscriptionMetrics("processor.logcluster-sampler", "agent.output").TotalLagRecords);
    }

    [TestMethod]
    public async Task Processor_RestartsFromLastCheckpoint_AfterFailure()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(ProcessorOptions(dir));
        await AppendInputAsync(host, "parse_failed-a", "parse_failed-b");

        Assert.AreEqual(
            2,
            await host.RunProcessorOnceAsync("logcluster-sampler", "agent.output", new SamplerProcessor()));

        await AppendInputAsync(host, "parse_failed-c", "parse_failed-d");

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => host.RunProcessorOnceAsync(
                "logcluster-sampler", "agent.output", new FailAfterOutputProcessor()));

        // The failed run stopped at parse_failed-c without committing it; a
        // healthy rerun picks up exactly the two unprocessed records.
        Assert.AreEqual(
            2,
            await host.RunProcessorOnceAsync("logcluster-sampler", "agent.output", new SamplerProcessor()));

        Assert.AreEqual(
            0,
            host.GetSubscriptionMetrics("processor.logcluster-sampler", "agent.output").TotalLagRecords);
    }

    [TestMethod]
    public async Task SamplerDropping_DoesNotAffectArchiveSubscription()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(ProcessorOptions(dir));
        await AppendInputAsync(host, "ok-1", "ok-2", "parse_failed-1");

        await host.RunProcessorOnceAsync("logcluster-sampler", "agent.output", new SamplerProcessor());

        var archive = host.CreateConsumer<TestEvent>("archive");
        var records = await TestHost.ReadAllAsync(archive, "agent.output");
        Assert.AreEqual(3, records.Count, "sampler drops must never remove archive records");
    }
}
