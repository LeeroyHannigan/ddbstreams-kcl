using System.Diagnostics;
using System.Text.Json;

namespace Amazon.DynamoDBStreams.Consumer;

/// <summary>
/// Where a freshly-seeded shard (no checkpoint) begins reading:
/// <see cref="InitialPosition.TrimHorizon"/> (default) or <see cref="InitialPosition.Latest"/>.
/// </summary>
public enum InitialPosition
{
    /// <summary>Read the shard from the oldest available record (default).</summary>
    TrimHorizon,

    /// <summary>Read only records written after the consumer starts.</summary>
    Latest,
}

/// <summary>Configuration for a <see cref="Worker"/>.</summary>
public sealed class WorkerConfig
{
    /// <summary>The DynamoDB Streams ARN to consume (required).</summary>
    public string StreamArn { get; set; } = "";

    /// <summary>The DynamoDB table storing shard leases + checkpoints (required).</summary>
    public string LeaseTable { get; set; } = "";

    /// <summary>The record processor (required).</summary>
    public IRecordProcessor Processor { get; set; } = null!;

    /// <summary>Unique worker identity for lease ownership. Optional.</summary>
    public string? Owner { get; set; }

    /// <summary>AWS region. Optional (falls back to the standard AWS environment).</summary>
    public string? Region { get; set; }

    /// <summary>How attribute values are surfaced. Defaults to <see cref="RecordFormat.Native"/>.</summary>
    public RecordFormat RecordFormat { get; set; } = RecordFormat.Native;

    /// <summary>Maximum shard leases held at once. Optional.</summary>
    public int? MaxLeases { get; set; }

    /// <summary>Cap on shards processed concurrently (opt-in). Unset = one slot per shard.
    /// Bounds concurrent delivery so footprint stays O(max) as shard count grows;
    /// preserves at-least-once + per-item + per-shard ordering.</summary>
    public int? MaxProcessingConcurrency { get; set; }

    /// <summary>Lease duration in milliseconds. Optional.</summary>
    public long? LeaseDurationMs { get; set; }

    /// <summary>Idle poll backoff in milliseconds. Optional.</summary>
    public long? PollIntervalMs { get; set; }

    /// <summary>Coordination cycle interval in milliseconds. Optional.</summary>
    public long? CycleIntervalMs { get; set; }

    /// <summary>
    /// Where to start reading a shard that has no checkpoint. Optional.
    /// Allowed values: <c>TRIM_HORIZON</c> (default) reads from the oldest
    /// available record; <c>LATEST</c> reads only records written after the
    /// worker starts. Case-insensitive.
    /// </summary>
    public InitialPosition? InitialPosition { get; set; }

    /// <summary>Explicit sidecar binary path (overrides discovery). Optional.</summary>
    public string? SidecarPath { get; set; }

    /// <summary>Full launch argv (tests / custom launch; overrides discovery). Optional.</summary>
    public IReadOnlyList<string>? SidecarCmd { get; set; }
}

/// <summary>
/// A JVM-free DynamoDB Streams consumer. Spawns the shared Rust sidecar and
/// delivers ordered, checkpointed change records to an <see cref="IRecordProcessor"/>
/// over the JSON-Lines wire protocol.
/// </summary>
public sealed class Worker
{
    private readonly WorkerConfig _config;
    private Process? _proc;
    private volatile bool _closed;

    /// <summary>Create a worker from the given configuration.</summary>
    public Worker(WorkerConfig config)
    {
        if (config is null || string.IsNullOrEmpty(config.StreamArn)
            || string.IsNullOrEmpty(config.LeaseTable) || config.Processor is null)
        {
            throw new ArgumentException("StreamArn, LeaseTable and Processor are required.", nameof(config));
        }
        _config = config;
    }

    /// <summary>
    /// Run until the sidecar shuts down (stream fully consumed, <see cref="Stop"/>
    /// called, or fatal error). Returns the sidecar's exit code.
    /// </summary>
    public async Task<int> RunAsync()
    {
        var argv = _config.SidecarCmd is { Count: > 0 } cmd
            ? new List<string>(cmd)
            : new List<string> { await Sidecar.DiscoverAsync(_config.SidecarPath).ConfigureAwait(false) };

        var psi = new ProcessStartInfo
        {
            FileName = argv[0],
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = false, // inherit → sidecar logs to our stderr
            UseShellExecute = false,
        };
        for (var i = 1; i < argv.Count; i++)
        {
            psi.ArgumentList.Add(argv[i]);
        }
        ApplyEnv(psi);

        _proc = new Process { StartInfo = psi };
        _proc.Start();
        var stdin = _proc.StandardInput;
        var stdout = _proc.StandardOutput;

        Send(stdin, new { type = "ready" });

        string? line;
        while ((line = await stdout.ReadLineAsync().ConfigureAwait(false)) != null)
        {
            line = line.Trim();
            if (line.Length == 0)
            {
                continue;
            }

            JsonDocument doc;
            try
            {
                doc = JsonDocument.Parse(line);
            }
            catch (JsonException)
            {
                continue; // ignore malformed / non-protocol noise
            }

            using (doc)
            {
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object
                    || !root.TryGetProperty("type", out var typeEl))
                {
                    continue;
                }
                switch (typeEl.GetString())
                {
                    case "records":
                        HandleRecords(root, stdin);
                        break;
                    case "shard_complete":
                        if (root.TryGetProperty("shard", out var sc))
                        {
                            _config.Processor.ShardEnded(sc.GetString() ?? "");
                        }
                        break;
                    case "lease_lost":
                        if (root.TryGetProperty("shard", out var ll))
                        {
                            _config.Processor.LeaseLost(ll.GetString() ?? "");
                        }
                        break;
                    case "shutdown_requested":
                        if (root.TryGetProperty("shard", out var sr))
                        {
                            _config.Processor.ShutdownRequested(sr.GetString() ?? "");
                        }
                        break;
                    case "shutdown":
                        StopInternal(stdin);
                        break;
                    default:
                        break;
                }
            }
        }

        _proc.WaitForExit();
        _closed = true;
        return _proc.ExitCode;
    }

    /// <summary>Request a graceful shutdown; <see cref="RunAsync"/> resolves once the sidecar exits.</summary>
    public void Stop()
    {
        if (_proc is { } p && !_closed)
        {
            try
            {
                Send(p.StandardInput, new { type = "stop" });
            }
            catch (IOException)
            {
                // pipe already gone
            }
        }
    }

    private void HandleRecords(JsonElement root, StreamWriter stdin)
    {
        var shard = root.TryGetProperty("shard", out var sh) ? sh.GetString() ?? "" : "";
        var lastSeq = root.TryGetProperty("last_seq", out var ls) ? ls.GetString() : null;

        var records = new List<Record>();
        if (root.TryGetProperty("records", out var recs) && recs.ValueKind == JsonValueKind.Array)
        {
            foreach (var r in recs.EnumerateArray())
            {
                records.Add(RecordFromWire(shard, r, _config.RecordFormat));
            }
        }

        _config.Processor.ProcessRecords(records);
        Send(stdin, new { type = "checkpoint", shard, seq = lastSeq });
    }

    private static Record RecordFromWire(string shard, JsonElement w, RecordFormat fmt)
    {
        string? Str(string prop) =>
            w.TryGetProperty(prop, out var el) && el.ValueKind == JsonValueKind.String ? el.GetString() : null;

        Dictionary<string, object?>? Image(string prop)
        {
            if (!w.TryGetProperty(prop, out var el) || el.ValueKind != JsonValueKind.Object)
            {
                return null;
            }
            return fmt == RecordFormat.Sdk
                ? SdkAttributeValues.DecodeItem(el)
                : AttributeValueConverter.DecodeItem(el, fmt);
        }

        return new Record
        {
            ShardId = shard,
            EventName = Str("event_name"),
            SequenceNumber = Str("sequence_number"),
            StreamViewType = Str("stream_view_type"),
            Keys = Image("keys") ?? new Dictionary<string, object?>(),
            NewImage = Image("new_image"),
            OldImage = Image("old_image"),
        };
    }

    private void ApplyEnv(ProcessStartInfo psi)
    {
        var e = psi.Environment;
        e["DDB_STREAMS_CONSUMER_STREAM_ARN"] = _config.StreamArn;
        e["DDB_STREAMS_CONSUMER_LEASE_TABLE"] = _config.LeaseTable;
        if (!string.IsNullOrEmpty(_config.Owner))
        {
            e["DDB_STREAMS_CONSUMER_OWNER"] = _config.Owner!;
        }
        if (!string.IsNullOrEmpty(_config.Region))
        {
            e["AWS_REGION"] = _config.Region!;
        }
        if (_config.MaxLeases is { } ml)
        {
            e["DDB_STREAMS_CONSUMER_MAX_LEASES"] = ml.ToString();
        }
        if (_config.MaxProcessingConcurrency is { } mpc)
        {
            e["DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY"] = mpc.ToString();
        }
        if (_config.LeaseDurationMs is { } ld)
        {
            e["DDB_STREAMS_CONSUMER_LEASE_DURATION_MS"] = ld.ToString();
        }
        if (_config.PollIntervalMs is { } pi)
        {
            e["DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS"] = pi.ToString();
        }
        if (_config.CycleIntervalMs is { } ci)
        {
            e["DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS"] = ci.ToString();
        }
        if (_config.InitialPosition is { } ip)
        {
            e["DDB_STREAMS_CONSUMER_INITIAL_POSITION"] = ip switch
            {
                InitialPosition.Latest => "LATEST",
                _ => "TRIM_HORIZON",
            };
        }
    }

    private void StopInternal(StreamWriter stdin)
    {
        if (_closed)
        {
            return;
        }
        try
        {
            Send(stdin, new { type = "stop" });
        }
        catch (IOException)
        {
            // pipe gone
        }
        try
        {
            stdin.Close();
        }
        catch (IOException)
        {
            // already closed
        }
    }

    private static void Send(StreamWriter stdin, object msg)
    {
        // Best-effort: the sidecar may have already emitted its final message and
        // exited (closing its stdin), so a write can race into a broken pipe. That
        // is a normal shutdown, not an error — the read loop observes stdout EOF
        // and stops. Mirrors the other clients (Python swallows BrokenPipeError).
        try
        {
            stdin.Write(JsonSerializer.Serialize(msg));
            stdin.Write('\n');
            stdin.Flush();
        }
        catch (IOException)
        {
            // sidecar stdin already closed
        }
        catch (ObjectDisposedException)
        {
            // stdin already disposed
        }
    }
}
