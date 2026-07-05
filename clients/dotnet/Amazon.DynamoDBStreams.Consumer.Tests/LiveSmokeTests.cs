using Xunit;

namespace Amazon.DynamoDBStreams.Consumer.Tests;

// Live smoke test: runs the Worker against the REAL Rust sidecar and a REAL
// DynamoDB stream. Skipped unless DDB_STREAMS_CONSUMER_IT=1. The stream + lease
// table are provisioned out-of-band (see the shell harness) and passed via env:
//   DDB_STREAMS_CONSUMER_STREAM_ARN, DDB_STREAMS_CONSUMER_LEASE_TABLE,
//   DDB_STREAMS_CONSUMER_SIDECAR (path to the built binary), AWS_REGION.
public class LiveSmokeTests
{
    private sealed class Collector : IRecordProcessor
    {
        public readonly List<Record> Records = new();
        private readonly object _lock = new();

        public void ProcessRecords(IReadOnlyList<Record> records)
        {
            lock (_lock)
            {
                Records.AddRange(records);
            }
        }

        public int Count
        {
            get { lock (_lock) { return Records.Count; } }
        }
    }

    [Fact]
    public async Task LiveConsume()
    {
        var evidence = Path.Combine(Path.GetTempPath(), "adsc_dotnet_live.txt");
        if (Environment.GetEnvironmentVariable("DDB_STREAMS_CONSUMER_IT") is null)
        {
            File.WriteAllText(evidence, "skipped: DDB_STREAMS_CONSUMER_IT not set\n");
            return; // skipped unless explicitly enabled
        }

        var arn = Environment.GetEnvironmentVariable("DDB_STREAMS_CONSUMER_STREAM_ARN")!;
        var leaseTable = Environment.GetEnvironmentVariable("DDB_STREAMS_CONSUMER_LEASE_TABLE")!;
        Assert.False(string.IsNullOrEmpty(arn), "DDB_STREAMS_CONSUMER_STREAM_ARN must be set");
        Assert.False(string.IsNullOrEmpty(leaseTable), "DDB_STREAMS_CONSUMER_LEASE_TABLE must be set");

        var c = new Collector();
        var worker = new Worker(new WorkerConfig
        {
            StreamArn = arn,
            LeaseTable = leaseTable,
            Processor = c,
            Region = Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1",
            RecordFormat = RecordFormat.Native,
            PollIntervalMs = 200,
        });

        var run = worker.RunAsync();

        // Wait (bounded) for records to arrive, then stop gracefully.
        for (var i = 0; i < 60 && c.Count < 5; i++)
        {
            await Task.Delay(500);
        }
        worker.Stop();
        await run;

        File.WriteAllText(evidence, $"consumed={c.Count}\n");

        Assert.True(c.Count >= 5, $"expected >= 5 records, got {c.Count}");

        // Native decoding: keys expose a bare string for the partition key.
        var withKey = c.Records.First(r => r.Keys.ContainsKey("pk"));
        Assert.IsType<string>(withKey.Keys["pk"]);
    }
}
