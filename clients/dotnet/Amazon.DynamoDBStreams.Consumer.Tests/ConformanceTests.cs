using System.Runtime.CompilerServices;
using System.Text.Json;
using Xunit;

namespace Amazon.DynamoDBStreams.Consumer.Tests;

// Drives every shared conformance fixture (conformance/fixtures/*.json) through
// the real Worker against the shared replay_sidecar.py — no AWS, no real
// sidecar. Mirrors clients/{python,go,node} conformance runners.
public class ConformanceTests
{
    private static readonly string ConfDir = ComputeConfDir();
    private static readonly string FixturesDir = Path.Combine(ConfDir, "fixtures");
    private static readonly string Replay = Path.Combine(ConfDir, "replay_sidecar.py");

    private static string ComputeConfDir([CallerFilePath] string thisFile = "")
    {
        // thisFile: <repo>/clients/dotnet/Amazon.DynamoDBStreams.Consumer.Tests/ConformanceTests.cs
        var dir = Path.GetDirectoryName(thisFile)!;
        return Path.GetFullPath(Path.Combine(dir, "..", "..", "..", "conformance"));
    }

    public static IEnumerable<object[]> Fixtures()
    {
        foreach (var f in Directory.GetFiles(FixturesDir, "*.json"))
        {
            yield return new object[] { Path.GetFileName(f) };
        }
    }

    private sealed class Collector : IRecordProcessor
    {
        public Dictionary<string, List<string?>> ByShard { get; } = new();
        public List<string> Ended { get; } = new();

        public void ProcessRecords(IReadOnlyList<Record> records)
        {
            foreach (var r in records)
            {
                if (!ByShard.TryGetValue(r.ShardId, out var list))
                {
                    ByShard[r.ShardId] = list = new List<string?>();
                }
                list.Add(r.SequenceNumber);
            }
        }

        public void ShardEnded(string shardId) => Ended.Add(shardId);
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task Conformance(string fixtureFile)
    {
        var fpath = Path.Combine(FixturesDir, fixtureFile);
        using var doc = JsonDocument.Parse(File.ReadAllText(fpath));
        var expect = doc.RootElement.GetProperty("expect");

        var c = new Collector();
        var worker = new Worker(new WorkerConfig
        {
            StreamArn = "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026",
            LeaseTable = "leases",
            Processor = c,
            SidecarCmd = new[] { "python3", Replay, fpath },
        });

        var code = await worker.RunAsync();

        // Checkpointing: replay exits non-zero on a wrong/absent ack.
        Assert.Equal(0, code);

        // Delivery counts + no extra shards.
        var expCounts = expect.GetProperty("records_per_shard");
        Assert.Equal(expCounts.EnumerateObject().Count(), c.ByShard.Count);
        foreach (var p in expCounts.EnumerateObject())
        {
            var got = c.ByShard.TryGetValue(p.Name, out var l) ? l.Count : 0;
            Assert.Equal(p.Value.GetInt32(), got);
        }

        // Per-shard order.
        foreach (var p in expect.GetProperty("record_order").EnumerateObject())
        {
            var order = p.Value.EnumerateArray().Select(e => e.GetString()).ToList();
            var got = c.ByShard.TryGetValue(p.Name, out var l) ? l : new List<string?>();
            Assert.Equal(order, got);
        }

        // Lifecycle: shard_ended.
        var expEnded = expect.GetProperty("shard_ended").EnumerateArray()
            .Select(e => e.GetString()!).OrderBy(x => x).ToList();
        Assert.Equal(expEnded, c.Ended.OrderBy(x => x).ToList());
    }
}
