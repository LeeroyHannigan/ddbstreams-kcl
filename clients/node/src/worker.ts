import { spawn, ChildProcess } from 'node:child_process';
import * as readline from 'node:readline';
import { recordFromWire, Record, RecordFormat } from './record';
import { discoverSidecar, VERSION } from './sidecar';

export { VERSION };

export interface RecordProcessor {
  processRecords(records: Record[]): void;
  shardEnded?(shardId: string): void;
  /**
   * Called when this worker loses a shard's lease (another worker took it, or
   * the lease expired). Optional. Do NOT checkpoint here — the shard is no
   * longer owned by this worker.
   */
  leaseLost?(shardId: string): void | Promise<void>;
  /**
   * Called when the sidecar asks this worker to wind down a shard (graceful
   * shutdown). Optional. This is a signal only — do NOT checkpoint here; the
   * last acked position has already been committed.
   */
  shutdownRequested?(shardId: string): void | Promise<void>;
}

export interface WorkerConfig {
  streamArn: string;
  leaseTable: string;
  processor: RecordProcessor;
  owner?: string;
  region?: string;
  /** How attribute values are exposed: 'native' (default) or 'ddb_json'. */
  recordFormat?: RecordFormat;
  maxLeases?: number;
  leaseDurationMs?: number;
  pollIntervalMs?: number;
  cycleIntervalMs?: number;
  /** Cap on shards processed concurrently (opt-in). Unset = one slot per shard.
   *  Bounds concurrent record delivery to keep footprint O(max) as shard count grows;
   *  preserves at-least-once + per-item + per-shard ordering. */
  maxProcessingConcurrency?: number;
  /** Where to start reading a shard with no checkpoint: `InitialPosition.TrimHorizon`
   *  (default) or `InitialPosition.Latest`. A bare `'TRIM_HORIZON'`/`'LATEST'` also works. */
  initialPosition?: InitialPosition;
  /** Explicit sidecar binary path (overrides discovery). */
  sidecarPath?: string;
  /** Full launch argv (tests / custom launch; overrides discovery). */
  sidecarCmd?: string[];
}

/**
 * Where a freshly-seeded shard (no checkpoint) begins reading. Reference a named
 * value (`InitialPosition.Latest`) or pass the bare string (`'LATEST'`) — both
 * type-check. Forwarded verbatim to the sidecar.
 */
export const InitialPosition = {
  TrimHorizon: 'TRIM_HORIZON',
  Latest: 'LATEST',
} as const;
export type InitialPosition = (typeof InitialPosition)[keyof typeof InitialPosition];

interface ServerMessage {
  type: string;
  shard?: string;
  last_seq?: string;
  records?: unknown[];
}

// A JVM-free DynamoDB Streams consumer. Embeds the shared Rust sidecar and
// delivers ordered, checkpointed change records to a processor over the
// JSON-Lines wire protocol.
export class Worker {
  private readonly config: WorkerConfig;
  private child: ChildProcess | null = null;
  private closed = false;

  constructor(config: WorkerConfig) {
    if (!config || !config.streamArn || !config.leaseTable || !config.processor) {
      throw new Error('streamArn, leaseTable and processor are required');
    }
    this.config = config;
  }

  private env(): NodeJS.ProcessEnv {
    const c = this.config;
    const env: NodeJS.ProcessEnv = { ...process.env };
    env.DDB_STREAMS_CONSUMER_STREAM_ARN = c.streamArn;
    env.DDB_STREAMS_CONSUMER_LEASE_TABLE = c.leaseTable;
    if (c.owner) env.DDB_STREAMS_CONSUMER_OWNER = c.owner;
    if (c.region) env.AWS_REGION = c.region;
    if (c.maxLeases != null) env.DDB_STREAMS_CONSUMER_MAX_LEASES = String(c.maxLeases);
    if (c.leaseDurationMs != null) env.DDB_STREAMS_CONSUMER_LEASE_DURATION_MS = String(c.leaseDurationMs);
    if (c.pollIntervalMs != null) env.DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS = String(c.pollIntervalMs);
    if (c.cycleIntervalMs != null) env.DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS = String(c.cycleIntervalMs);
    if (c.maxProcessingConcurrency != null) env.DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY = String(c.maxProcessingConcurrency);
    if (c.initialPosition != null) env.DDB_STREAMS_CONSUMER_INITIAL_POSITION = String(c.initialPosition).trim().toUpperCase();
    return env;
  }

  private send(msg: unknown): void {
    const stdin = this.child?.stdin;
    if (this.closed || !stdin || !stdin.writable) return;
    stdin.write(JSON.stringify(msg) + '\n');
  }

  /** Requests a graceful shutdown; run() resolves once the sidecar exits. */
  stop(): void {
    this.send({ type: 'stop' });
  }

  /** Runs until the sidecar shuts down. Resolves with the sidecar exit code. */
  async run(): Promise<number> {
    const argv =
      this.config.sidecarCmd && this.config.sidecarCmd.length
        ? this.config.sidecarCmd
        : [await discoverSidecar(this.config.sidecarPath)];

    const child = spawn(argv[0], argv.slice(1), {
      env: this.env(),
      stdio: ['pipe', 'pipe', 'inherit'], // sidecar logs to our stderr
    });
    this.child = child;
    if (!child.stdout) throw new Error('sidecar stdout not available');
    const rl = readline.createInterface({ input: child.stdout });

    this.send({ type: 'ready' });

    return new Promise<number>((resolve, reject) => {
      rl.on('line', (raw: string) => {
        const line = raw.trim();
        if (!line) return;
        let msg: ServerMessage;
        try {
          msg = JSON.parse(line) as ServerMessage;
        } catch {
          return; // ignore malformed / non-protocol noise
        }
        switch (msg.type) {
          case 'records': {
            const shard = msg.shard as string;
            const recs = (msg.records ?? []).map((r) =>
              recordFromWire(shard, r as never, this.config.recordFormat ?? 'native'),
            );
            this.config.processor.processRecords(recs);
            this.send({ type: 'checkpoint', shard, seq: msg.last_seq });
            break;
          }
          case 'shard_complete':
            this.config.processor.shardEnded?.(msg.shard as string);
            break;
          case 'lease_lost':
            // Lease for this shard was lost; surface to the processor but do
            // NOT checkpoint — the shard is no longer owned by this worker.
            this.config.processor.leaseLost?.(msg.shard as string);
            break;
          case 'shutdown_requested':
            // Sidecar asked us to wind down this shard; surface to the
            // processor but do NOT checkpoint — this is a signal only.
            this.config.processor.shutdownRequested?.(msg.shard as string);
            break;
          case 'shutdown':
            this.stopInternal();
            break;
          default:
            break;
        }
      });
      child.on('error', reject);
      child.on('close', (code: number | null) => {
        this.closed = true;
        resolve(code == null ? -1 : code);
      });
    });
  }

  private stopInternal(): void {
    if (this.closed || !this.child) return;
    try {
      this.send({ type: 'stop' });
    } catch {
      /* pipe gone */
    }
    try {
      this.child.stdin?.end();
    } catch {
      /* closed */
    }
  }
}
