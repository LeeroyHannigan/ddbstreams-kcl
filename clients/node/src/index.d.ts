export type AttrValue = string | boolean | null | Buffer | AttrValue[] | { [k: string]: AttrValue };
export type Item = { [k: string]: AttrValue };

export interface Record {
  shardId: string;
  eventName: string | null;
  sequenceNumber: string | null;
  streamViewType: string | null;
  keys: Item;
  newImage: Item | null;
  oldImage: Item | null;
}

export interface RecordProcessor {
  processRecords(records: Record[]): void;
  shardEnded?(shardId: string): void;
}

export interface WorkerConfig {
  streamArn: string;
  leaseTable: string;
  processor: RecordProcessor;
  owner?: string;
  region?: string;
  maxLeases?: number;
  leaseDurationMs?: number;
  pollIntervalMs?: number;
  cycleIntervalMs?: number;
  sidecarPath?: string;
  sidecarCmd?: string[];
}

export class Worker {
  constructor(config: WorkerConfig);
  /** Runs until the sidecar shuts down; resolves with its exit code. */
  run(): Promise<number>;
  /** Requests a graceful shutdown. */
  stop(): void;
}

export const VERSION: string;
export function decodeAttr(v: unknown): AttrValue;
export function decodeItem(item: Record<string, unknown> | null | undefined): Item;
export function recordFromWire(shard: string, wire: Record<string, unknown>): Record;
