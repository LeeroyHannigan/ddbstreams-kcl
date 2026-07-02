"""Client tests: wire decoding (unit) and the full Worker loop against a fake
sidecar over real subprocess stdio (no AWS needed)."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ddbstreams_kcl import Record, Worker, decode_attr  # noqa: E402
from ddbstreams_kcl.record import decode_item  # noqa: E402

FAKE = os.path.join(os.path.dirname(__file__), "fake_sidecar.py")


class TestDecode(unittest.TestCase):
    def test_scalar_and_null(self):
        self.assertEqual(decode_attr({"S": "hi"}), "hi")
        self.assertEqual(decode_attr({"N": "42"}), "42")  # numbers stay strings
        self.assertEqual(decode_attr({"Bool": True}), True)
        self.assertIsNone(decode_attr("Null"))

    def test_collections(self):
        self.assertEqual(decode_attr({"B": [0, 1, 255]}), bytes([0, 1, 255]))
        self.assertEqual(decode_attr({"Ss": ["a", "b"]}), ["a", "b"])
        self.assertEqual(decode_attr({"Ns": ["1", "2.5"]}), ["1", "2.5"])
        self.assertEqual(
            decode_attr({"M": {"x": {"N": "1"}, "y": {"S": "z"}}}), {"x": "1", "y": "z"}
        )
        self.assertEqual(decode_attr({"L": [{"S": "a"}, "Null"]}), ["a", None])

    def test_record_from_wire(self):
        wire = {
            "event_name": "MODIFY",
            "sequence_number": "100",
            "stream_view_type": "NEW_AND_OLD_IMAGES",
            "keys": {"pk": {"S": "k1"}},
            "new_image": {"pk": {"S": "k1"}, "active": {"Bool": True}},
            "old_image": None,
        }
        r = Record.from_wire("shardId-1", wire)
        self.assertEqual(r.shard_id, "shardId-1")
        self.assertEqual(r.event_name, "MODIFY")
        self.assertEqual(r.keys, {"pk": "k1"})
        self.assertEqual(r.new_image, {"pk": "k1", "active": True})
        self.assertIsNone(r.old_image)

    def test_decode_item_empty(self):
        self.assertEqual(decode_item(None), {})
        self.assertEqual(decode_item({}), {})


class _Collector:
    def __init__(self):
        self.records = []
        self.ended = []

    def process_records(self, records):
        self.records.extend(records)

    def shard_ended(self, shard_id):
        self.ended.append(shard_id)


class TestWorkerAgainstFakeSidecar(unittest.TestCase):
    def test_full_loop_delivers_records_and_acks(self):
        proc = _Collector()
        worker = Worker(
            stream_arn="arn:aws:dynamodb:us-east-1:1:table/T/stream/2026",
            lease_table="leases",
            processor=proc,
            sidecar_cmd=[sys.executable, FAKE],
        )
        exit_code = worker.run()

        # The fake exits 0 ONLY if it received a correct checkpoint ack (seq ==
        # each batch's last_seq) for both shards — so a clean exit proves the ack
        # path end to end.
        self.assertEqual(exit_code, 0, "fake sidecar rejected the checkpoint acks")

        # All three records delivered, decoded, in per-shard order.
        self.assertEqual(len(proc.records), 3)
        seqs = [r.sequence_number for r in proc.records]
        self.assertEqual(seqs, ["1", "2", "9"])
        self.assertEqual(proc.records[0].keys, {"pk": "k1"})
        self.assertEqual(proc.records[0].new_image, {"pk": "k1", "active": True, "n": "42"})
        self.assertEqual(proc.records[2].shard_id, "s1")


if __name__ == "__main__":
    unittest.main()
