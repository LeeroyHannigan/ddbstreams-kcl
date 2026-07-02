#!/usr/bin/env python3
"""Live smoke: run the Python Worker against the REAL sidecar binary and a real
DynamoDB stream. Collects records until a target count (or timeout), then stops
gracefully. Prints ``PYCLIENT_OK <count> <shards>`` on success.

Usage:
  DDBSTREAMS_KCL_SIDECAR=/path/to/ddbstreams-kcl-sidecar \\
  python3 live_smoke.py <stream_arn> <lease_table> <region> [target=5] [timeout=60]
"""

import sys
import threading
import time

sys.path.insert(0, __file__.rsplit("/examples/", 1)[0] + "/src")

from ddbstreams_kcl import Worker  # noqa: E402


class Collector:
    def __init__(self, target, worker_box):
        self.records = []
        self.shards = set()
        self.target = target
        self.worker_box = worker_box
        self.done = threading.Event()

    def process_records(self, records):
        self.records.extend(records)
        for r in records:
            self.shards.add(r.shard_id)
        if len(self.records) >= self.target:
            self.done.set()

    def shard_ended(self, shard_id):
        pass


def main():
    stream_arn, lease_table, region = sys.argv[1], sys.argv[2], sys.argv[3]
    target = int(sys.argv[4]) if len(sys.argv) > 4 else 5
    timeout = float(sys.argv[5]) if len(sys.argv) > 5 else 60.0

    box = {}
    coll = Collector(target, box)
    worker = Worker(
        stream_arn=stream_arn,
        lease_table=lease_table,
        processor=coll,
        region=region,
        owner="py-smoke",
        lease_duration_ms=60000,
        cycle_interval_ms=500,
    )
    box["w"] = worker

    t = threading.Thread(target=worker.run, daemon=True)
    t.start()

    got = coll.done.wait(timeout=timeout)
    worker.stop()
    t.join(timeout=10)

    if got and len(coll.records) >= target:
        print(f"PYCLIENT_OK {len(coll.records)} {len(coll.shards)}")
        sys.exit(0)
    print(f"PYCLIENT_FAIL got={len(coll.records)} target={target}")
    sys.exit(1)


if __name__ == "__main__":
    main()
