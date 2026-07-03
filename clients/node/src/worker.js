'use strict';

const { spawn } = require('node:child_process');
const readline = require('node:readline');
const { recordFromWire } = require('./record');
const { discoverSidecar, VERSION } = require('./sidecar');

// A JVM-free DynamoDB Streams consumer. Embeds the shared Rust sidecar (which
// owns shard discovery, leasing, ordering, checkpointing) and delivers ordered,
// checkpointed change records to a processor. Thin stdio bridge over the
// JSON-Lines wire protocol (protocol/src/lib.rs).
//
// config:
//   streamArn, leaseTable, processor         (required)
//   owner, region, maxLeases, leaseDurationMs, pollIntervalMs, cycleIntervalMs
//   sidecarPath   explicit binary path
//   sidecarCmd    full launch argv (tests / custom launch)
//
// processor: { processRecords(records), shardEnded?(shardId) }
class Worker {
  constructor(config) {
    if (!config || !config.streamArn || !config.leaseTable || !config.processor) {
      throw new Error('streamArn, leaseTable and processor are required');
    }
    this.config = config;
    this._child = null;
    this._closed = false;
  }

  _env() {
    const c = this.config;
    const env = { ...process.env };
    env.DDB_STREAMS_CONSUMER_STREAM_ARN = c.streamArn;
    env.DDB_STREAMS_CONSUMER_LEASE_TABLE = c.leaseTable;
    if (c.owner) env.DDB_STREAMS_CONSUMER_OWNER = c.owner;
    if (c.region) env.AWS_REGION = c.region;
    if (c.maxLeases != null) env.DDB_STREAMS_CONSUMER_MAX_LEASES = String(c.maxLeases);
    if (c.leaseDurationMs != null) env.DDB_STREAMS_CONSUMER_LEASE_DURATION_MS = String(c.leaseDurationMs);
    if (c.pollIntervalMs != null) env.DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS = String(c.pollIntervalMs);
    if (c.cycleIntervalMs != null) env.DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS = String(c.cycleIntervalMs);
    return env;
  }

  _send(msg) {
    if (this._closed || !this._child || !this._child.stdin.writable) return;
    this._child.stdin.write(JSON.stringify(msg) + '\n');
  }

  // Request a graceful shutdown from elsewhere; run() resolves once the sidecar exits.
  stop() {
    this._send({ type: 'stop' });
  }

  // Runs until the sidecar shuts down. Resolves with the sidecar exit code.
  async run() {
    const argv =
      this.config.sidecarCmd && this.config.sidecarCmd.length
        ? this.config.sidecarCmd
        : [await discoverSidecar(this.config.sidecarPath)];

    const child = spawn(argv[0], argv.slice(1), {
      env: this._env(),
      stdio: ['pipe', 'pipe', 'inherit'], // sidecar logs to our stderr
    });
    this._child = child;
    const rl = readline.createInterface({ input: child.stdout });

    this._send({ type: 'ready' });

    return new Promise((resolve, reject) => {
      rl.on('line', (raw) => {
        const line = raw.trim();
        if (!line) return;
        let msg;
        try {
          msg = JSON.parse(line);
        } catch {
          return; // ignore malformed / non-protocol noise
        }
        switch (msg.type) {
          case 'records': {
            const recs = (msg.records || []).map((r) => recordFromWire(msg.shard, r));
            this.config.processor.processRecords(recs);
            // Ack: durably processed up to last_seq -> sidecar checkpoints it.
            this._send({ type: 'checkpoint', shard: msg.shard, seq: msg.last_seq });
            break;
          }
          case 'shard_complete':
            if (typeof this.config.processor.shardEnded === 'function') {
              this.config.processor.shardEnded(msg.shard);
            }
            break;
          case 'shutdown':
            this._stop();
            break;
          default:
            break;
        }
      });
      child.on('error', reject);
      child.on('close', (code) => {
        this._closed = true;
        resolve(code == null ? -1 : code);
      });
    });
  }

  _stop() {
    if (this._closed || !this._child) return;
    try {
      this._send({ type: 'stop' });
    } catch {
      /* pipe may be gone */
    }
    try {
      this._child.stdin.end();
    } catch {
      /* already closed */
    }
  }
}

module.exports = { Worker, VERSION };
