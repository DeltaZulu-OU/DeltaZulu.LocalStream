namespace DeltaZulu.LocalStream.Tests;

internal sealed record TestEvent(string Source, string Message);

internal static class TestHost
{
    public static string NewStorageDir(TestContext context)
    {
        var dir = Path.Combine(
            context.TestRunDirectory ?? Path.GetTempPath(),
            "localstream",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    public static LocalStreamOptions Options(
        string storagePath,
        string topic = "agent.output",
        int partitions = 1,
        long? retentionMaxBytes = null,
        TimeSpan? retentionMaxAge = null,
        long maxSegmentBytes = 1024 * 1024)
    {
        return new LocalStreamOptions
        {
            StoragePath = storagePath,
            Topics =
            {
                new TopicOptions
                {
                    Name = topic,
                    Partitions = partitions,
                    MaxSegmentBytes = maxSegmentBytes,
                    Retention = new RetentionOptions
                    {
                        MaxBytes = retentionMaxBytes,
                        MaxAge = retentionMaxAge,
                    },
                },
            },
        };
    }

    public static async Task<LocalStreamHost> StartAsync(LocalStreamOptions options)
    {
        var host = new LocalStreamHost(options);
        await host.StartAsync();
        return host;
    }

    public static async Task<IReadOnlyList<StreamRecord<TestEvent>>> ReadAllAsync(
        ILocalStreamConsumer<TestEvent> consumer,
        string topic,
        ReadOptions? options = null)
    {
        var records = new List<StreamRecord<TestEvent>>();
        await foreach (var record in consumer.ReadAsync(topic, options))
        {
            records.Add(record);
        }

        return records;
    }
}
