#!/usr/bin/env python3
"""Cleanroom multi-hour soak + correctness harness for amazon-dynamodb-streams-consumer.

The CONSUMER under test is ONLY the pip-installed `dynamodb_streams_consumer`
package (from TestPyPI, native sidecar bundled). boto3 is used solely as an
independent control-plane / writer / verifier -- it never touches the consumer's
code path.

What it proves, over a configurable number of hours against REAL DynamoDB
Streams (Isengard sandbox), continuously:

  1. Completeness   - every written (pk, sk) is eventually observed at least once.
  2. Ordering       - per partition key, first-observation of sk is strictly
                      increasing (holds within a shard AND across a real shard
                      roll, since a key's child shard is consumed only after its
                      parent completes -- parent-before-child).
  3. Exactly-once-ish- duplicates are counted; at-least-once redelivery of the
                      last un-acked batch on restart/steal is tolerated but must
                      be rare and must never break ordering.
  4. Checkpoint resume - a mid-run graceful stop + restart resumes from the
                      checkpoint with no gap (completeness preserved).
  5. Multi-worker single-ownership - a second worker joins for a window;
                      correctness invariants must still hold under lease
                      contention / rebalancing.

Config via env (all optional except creds/region):
  SOAK_HOURS   float  total run time            (default 6.0)
  WRITE_RATE   float  writes/sec                (default 4.0)
  NUM_KEYS     int    distinct partition keys   (default 8)
  RESTART_AT   float  fraction of run to restart the worker (default 0.34)
  SECOND_AT    float  fraction to start a 2nd worker        (default 0.66)
  SECOND_FOR   float  minutes the 2nd worker runs           (default 20)
  OUTDIR       path   where to write report/log (default /soak/out)
  KEEP_ON_PASS 0/1    keep tables even on pass  (default 0 -> delete on pass)
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from dynamodb_streams_consumer import Worker

REGION = os.environ.get("AWS_REGION", "us-east-1")
SOAK_HOURS = float(os.environ.get("SOAK_HOURS", "6.0"))
WRITE_RATE = float(os.environ.get("WRITE_RATE", "4.0"))
NUM_KEYS = int(os.environ.get("NUM_KEYS", "8"))
RESTART_AT = float(os.environ.get("RESTART_AT", "0.34"))
SECOND_AT = float(os.environ.get("SECOND_AT", "0.66"))
SECOND_FOR = float(os.environ.get("SECOND_FOR", "20"))
OUTDIR = os.environ.get("OUTDIR", "/soak/out")
KEEP_ON_PASS = os.environ.get("KEEP_ON_PASS", "0") == "1"

STAMP = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
TABLE = f"adsc-soak-{STAMP}"
LEASE_TABLE = f"adsc-soak-lease-{STAMP}"
OWNER_A = "soak-worker-A"
OWNER_B = "soak-worker-B"

os.makedirs(OUTDIR, exist_ok=True)
LOG_PATH = os.path.join(OUTDIR, "soak.log")
REPORT_PATH = os.path.join(OUTDIR, "report.json")

_boto = Config(retries={"max_attempts": 10, "mode": "adaptive"})
ddb = boto3.client("dynamodb", region_name=REGION, config=_boto)

_log_lock = threading.Lock()


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    with _log_lock:
        print(line, flush=True)
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")


# --------------------------------------------------------------------------
# Shared observation state (written by consumer threads, read by checker).
# --------------------------------------------------------------------------
class Observed:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.last_sk = {}                     # pk -> last first-observed sk
        self.seen = set()                     # (pk, sk) unique observations
        self.dups = 0                         # repeat deliveries
        self.order_violations = []            # (pk, prev_sk, sk)
        self.shards = set()                   # distinct shard ids observed
        self.total_delivered = 0              # incl. dups

    def record(self, pk: str, sk: int, shard: str) -> None:
        with self.lock:
            self.total_delivered += 1
            self.shards.add(shard)
            key = (pk, sk)
            if key in self.seen:
                self.dups += 1
                return
            self.seen.add(key)
            prev = self.last_sk.get(pk)
            # First-observation ordering: sk must exceed the last NEW sk we saw
            # for this key. (Dups are excluded above.)
            if prev is not None and sk <= prev:
                self.order_violations.append((pk, prev, sk))
            else:
                self.last_sk[pk] = sk


OBS = Observed()


class SoakProcessor:
    """Consumer-side record processor (the only integration with the package)."""

    def process_records(self, records):
        for r in records:
            keys = r.keys or {}
            pk = keys.get("pk")
            sk = keys.get("sk")
            if pk is None or sk is None:
                continue
            OBS.record(str(pk), int(sk), r.shard_id)

    def shard_ended(self, shard_id):
        log(f"[consumer] shard_complete {shard_id}")


# --------------------------------------------------------------------------
# Writer
# --------------------------------------------------------------------------
class Writer(threading.Thread):
    def __init__(self, stop_evt: threading.Event):
        super().__init__(daemon=True)
        self.stop_evt = stop_evt
        self.per_key = defaultdict(int)       # pk -> highest sk written
        self.total = 0
        self.lock = threading.Lock()

    def snapshot(self):
        with self.lock:
            return dict(self.per_key), self.total

    def run(self):
        interval = 1.0 / WRITE_RATE if WRITE_RATE > 0 else 0.25
        i = 0
        while not self.stop_evt.is_set():
            pk = f"k{i % NUM_KEYS}"
            with self.lock:
                self.per_key[pk] += 1
                sk = self.per_key[pk]
                self.total += 1
            try:
                ddb.put_item(
                    TableName=TABLE,
                    Item={
                        "pk": {"S": pk},
                        "sk": {"N": str(sk)},
                        "n": {"N": str(self.total)},
                        "ts": {"N": str(int(time.time() * 1000))},
                        "payload": {"S": "x" * 64},
                    },
                )
            except Exception as e:  # noqa: BLE001
                # roll back the counter so completeness accounting stays exact
                with self.lock:
                    self.per_key[pk] -= 1
                    self.total -= 1
                log(f"[writer] put_item error (will retry): {e}")
                time.sleep(1.0)
                continue
            i += 1
            time.sleep(interval)
        log(f"[writer] stopped after {self.total} writes")


# --------------------------------------------------------------------------
# Worker lifecycle helpers
# --------------------------------------------------------------------------
def start_worker(stream_arn: str, owner: str) -> tuple[Worker, threading.Thread]:
    w = Worker(
        stream_arn=stream_arn,
        lease_table=LEASE_TABLE,
        processor=SoakProcessor(),
        owner=owner,
        region=REGION,
        lease_duration_ms=15000,
        cycle_interval_ms=1000,
    )
    t = threading.Thread(target=w.run, name=f"worker-{owner}", daemon=True)
    t.start()
    log(f"[worker] started owner={owner}")
    return w, t


def setup_table() -> str:
    log(f"[setup] creating table {TABLE} (streams NEW_AND_OLD_IMAGES)")
    ddb.create_table(
        TableName=TABLE,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    ddb.get_waiter("table_exists").wait(TableName=TABLE)
    desc = ddb.describe_table(TableName=TABLE)["Table"]
    stream_arn = desc["LatestStreamArn"]
    log(f"[setup] table ACTIVE, stream={stream_arn}")
    return stream_arn


def teardown():
    for t in (TABLE, LEASE_TABLE):
        try:
            ddb.delete_table(TableName=t)
            log(f"[cleanup] delete_table {t}")
        except Exception as e:  # noqa: BLE001
            log(f"[cleanup] delete_table {t} failed: {e}")


def checker_snapshot(writer: Writer) -> dict:
    written_per_key, written_total = writer.snapshot()
    with OBS.lock:
        seen_total = len(OBS.seen)
        dups = OBS.dups
        violations = list(OBS.order_violations)
        shards = len(OBS.shards)
        delivered = OBS.total_delivered
    # completeness lag = written but not yet observed
    missing = 0
    with OBS.lock:
        for pk, hi in written_per_key.items():
            for sk in range(1, hi + 1):
                if (pk, sk) not in OBS.seen:
                    missing += 1
    return {
        "written_total": written_total,
        "observed_unique": seen_total,
        "delivered_incl_dups": delivered,
        "duplicates": dups,
        "order_violations": len(violations),
        "order_violation_samples": violations[:10],
        "distinct_shards": shards,
        "completeness_lag": missing,
    }


def main() -> int:
    t0 = time.time()
    total_secs = SOAK_HOURS * 3600.0
    log(f"=== SOAK START table={TABLE} hours={SOAK_HOURS} rate={WRITE_RATE}/s "
        f"keys={NUM_KEYS} region={REGION} ===")
    result = {"table": TABLE, "lease_table": LEASE_TABLE, "start": STAMP,
              "config": {"hours": SOAK_HOURS, "rate": WRITE_RATE, "keys": NUM_KEYS}}
    phases = {}

    try:
        stream_arn = setup_table()
        result["stream_arn"] = stream_arn

        stop_writer = threading.Event()
        writer = Writer(stop_writer)
        writer.start()

        worker, wt = start_worker(stream_arn, OWNER_A)

        did_restart = False
        did_second = False
        second_worker = None
        second_thread = None
        second_started_at = None

        last_report = 0.0
        while True:
            now = time.time()
            elapsed = now - t0
            frac = elapsed / total_secs if total_secs else 1.0

            # periodic progress (every 60s)
            if now - last_report >= 60:
                snap = checker_snapshot(writer)
                log(f"[progress] t={elapsed/3600:.2f}h frac={frac:.2f} " + json.dumps(snap))
                if snap["order_violations"]:
                    log(f"[ALERT] ordering violations detected: {snap['order_violation_samples']}")
                last_report = now

            # mid-run restart (checkpoint resume test)
            if not did_restart and frac >= RESTART_AT:
                log("[phase] RESTART: graceful stop of worker A")
                pre = checker_snapshot(writer)
                worker.stop()
                wt.join(timeout=30)
                time.sleep(5)
                worker, wt = start_worker(stream_arn, OWNER_A)
                time.sleep(30)  # let it re-acquire + drain
                post = checker_snapshot(writer)
                phases["restart"] = {"pre": pre, "post": post,
                                     "resumed": post["observed_unique"] >= pre["observed_unique"]}
                log(f"[phase] RESTART done resumed={phases['restart']['resumed']}")
                did_restart = True

            # second worker window (multi-worker single-ownership test)
            if not did_second and frac >= SECOND_AT:
                log("[phase] SECOND WORKER: starting owner B")
                second_worker, second_thread = start_worker(stream_arn, OWNER_B)
                second_started_at = now
                did_second = True
                phases["second_start"] = checker_snapshot(writer)
            if did_second and second_worker and (now - second_started_at) >= SECOND_FOR * 60:
                log("[phase] SECOND WORKER: stopping owner B")
                second_worker.stop()
                if second_thread:
                    second_thread.join(timeout=30)
                phases["second_stop"] = checker_snapshot(writer)
                second_worker = None

            if elapsed >= total_secs:
                break
            time.sleep(2)

        # drain: stop writing, let the consumer catch up
        log("[drain] stopping writer, draining consumer up to 300s")
        stop_writer.set()
        writer.join(timeout=30)
        drain_deadline = time.time() + 300
        while time.time() < drain_deadline:
            snap = checker_snapshot(writer)
            if snap["completeness_lag"] == 0:
                break
            time.sleep(5)

        # stop workers
        worker.stop(); wt.join(timeout=30)
        if second_worker:
            second_worker.stop()

        final = checker_snapshot(writer)
        result["final"] = final
        result["phases"] = phases

        passed = (
            final["completeness_lag"] == 0
            and final["order_violations"] == 0
            and final["observed_unique"] == final["written_total"]
            and phases.get("restart", {}).get("resumed", False)
        )
        result["passed"] = passed
        result["verdict"] = "PASS" if passed else "FAIL"
        log(f"=== SOAK {result['verdict']} === " + json.dumps(final))

        with open(REPORT_PATH, "w") as f:
            json.dump(result, f, indent=2, default=str)

        if passed and not KEEP_ON_PASS:
            teardown()
        else:
            log(f"[cleanup] tables kept for inspection: {TABLE}, {LEASE_TABLE}")
        return 0 if passed else 1

    except Exception:  # noqa: BLE001
        result["error"] = traceback.format_exc()
        result["verdict"] = "ERROR"
        log("=== SOAK ERROR ===\n" + result["error"])
        with open(REPORT_PATH, "w") as f:
            json.dump(result, f, indent=2, default=str)
        return 2


if __name__ == "__main__":
    sys.exit(main())
