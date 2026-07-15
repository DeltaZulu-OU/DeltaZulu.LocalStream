namespace DeltaZulu.LocalStream.Tests;

[TestClass]
public sealed class ProcessorLoopTests
{
    public TestContext TestContext { get; set; } = null!;

    private static LocalStreamOptions LoopOptions(string dir)
    {
        var options = new LocalStreamOptions { StoragePath = dir };
        options.Topics.Add(new TopicOptions { Name = "agent.output" });
        options.Topics.Add(new TopicOptions { Name = "logcluster.samples" });
        return options;
    }

    private sealed class ForwardingProcessor : ILocalStreamProcessor<TestEvent, TestEvent>
    {
        public async ValueTask ProcessAsync(
            StreamRecord<TestEvent> input,
            ILocalStreamProducer<TestEvent> output,
            ProcessorContext context,
            CancellationToken cancellationToken = default)
        {
            await output.AppendAsync(
                "logcluster.samples",
                input.Payload,
                new AppendOptions { EventId = $"fwd-{input.EventId}" },
                cancellationToken);
        }
    }

    private static async Task<int> CountSamplesAsync(LocalStreamHost host)
    {
        var consumer = host.CreateConsumer<TestEvent>("debug-local");
        var records = await TestHost.ReadAllAsync(
            consumer, "logcluster.samples", new ReadOptions { Start = ReadStart.Earliest });
        return records.Count;
    }

    private static async Task WaitForSamplesAsync(LocalStreamHost host, int expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (await CountSamplesAsync(host) >= expected)
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.Fail($"Timed out waiting for {expected} samples; saw {await CountSamplesAsync(host)}.");
    }

    [TestMethod]
    public async Task RunProcessorAsync_ProcessesRecordsAppendedWhileRunning()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(LoopOptions(dir));
        var producer = host.CreateProducer<TestEvent>();

        await producer.AppendAsync("agent.output", new TestEvent("s", "before-loop"));

        using var cts = new CancellationTokenSource();
        var loop = host.RunProcessorAsync(
            "forwarder",
            "agent.output",
            new ForwardingProcessor(),
            pollInterval: TimeSpan.FromMilliseconds(20),
            cancellationToken: cts.Token);

        // Records existing before the loop started are drained...
        await WaitForSamplesAsync(host, 1);

        // ...and records appended while it runs are picked up on later polls.
        await producer.AppendAsync("agent.output", new TestEvent("s", "while-running-1"));
        await producer.AppendAsync("agent.output", new TestEvent("s", "while-running-2"));
        await WaitForSamplesAsync(host, 3);

        cts.Cancel();
        await loop;

        Assert.AreEqual(
            0,
            host.GetSubscriptionMetrics("processor.forwarder", "agent.output").TotalLagRecords);
    }

    [TestMethod]
    public async Task RunProcessorAsync_CompletesQuietly_WhenCancelledWhileIdle()
    {
        var dir = TestHost.NewStorageDir(TestContext);
        await using var host = await TestHost.StartAsync(LoopOptions(dir));

        using var cts = new CancellationTokenSource();
        var loop = host.RunProcessorAsync(
            "forwarder",
            "agent.output",
            new ForwardingProcessor(),
            pollInterval: TimeSpan.FromMilliseconds(20),
            cancellationToken: cts.Token);

        await Task.Delay(100);
        cts.Cancel();

        // Cancellation is the normal shutdown path, not an error.
        await loop;
    }
}
