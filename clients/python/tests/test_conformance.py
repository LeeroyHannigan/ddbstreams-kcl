"""Binding conformance: run every shared fixture in conformance/fixtures/
through the real Worker against the shared replay_sidecar.py, and assert the
fixture's `expect` block. This is the template each language binding mirrors.

No AWS: the replay sidecar is a local subprocess speaking the wire protocol.
"""
import glob
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dynamodb_streams_consumer import Worker  # noqa: E402

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CONF = os.path.join(_REPO, "conformance")
_REPLAY = os.path.join(_CONF, "replay_sidecar.py")
_FIXTURES = sorted(glob.glob(os.path.join(_CONF, "fixtures", "*.json")))


class _Collector:
    def __init__(self):
        self.by_shard = {}   # shard -> [sequence_number,...]
        self.ended = []

    def process_records(self, records):
        for r in records:
            self.by_shard.setdefault(r.shard_id, []).append(r.sequence_number)

    def shard_ended(self, shard_id):
        self.ended.append(shard_id)


def _make_case(fixture_path):
    def test(self):
        with open(fixture_path) as f:
            fixture = json.load(f)
        expect = fixture["expect"]
        proc = _Collector()
        exit_code = Worker(
            stream_arn="arn:aws:dynamodb:us-east-1:1:table/T/stream/2026",
            lease_table="leases",
            processor=proc,
            sidecar_cmd=[sys.executable, _REPLAY, fixture_path],
        ).run()

        # 2. Checkpointing: replay exits non-zero on a wrong/absent ack.
        self.assertEqual(exit_code, 0,
                         f"{fixture['name']}: replay rejected the checkpoint acks")

        # 1. Delivery: counts and per-shard sequence order.
        counts = {s: len(v) for s, v in proc.by_shard.items()}
        self.assertEqual(counts, {k: v for k, v in expect["records_per_shard"].items()},
                         f"{fixture['name']}: records_per_shard mismatch")
        for shard, order in expect["record_order"].items():
            self.assertEqual(proc.by_shard.get(shard, []), order,
                             f"{fixture['name']}: order mismatch on {shard}")

        # 3. Lifecycle: shard_ended fired for each shard_complete.
        self.assertEqual(sorted(proc.ended), sorted(expect["shard_ended"]),
                         f"{fixture['name']}: shard_ended mismatch")

    return test


class TestConformance(unittest.TestCase):
    pass


# Materialize one test method per fixture so failures are individually named.
assert _FIXTURES, f"no conformance fixtures found under {_CONF}/fixtures"
for _fx in _FIXTURES:
    _name = "test_" + os.path.splitext(os.path.basename(_fx))[0]
    setattr(TestConformance, _name, _make_case(_fx))


if __name__ == "__main__":
    unittest.main()
