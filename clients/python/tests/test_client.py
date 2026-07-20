"""Client tests: wire decoding (unit) and the full Worker loop against a fake
sidecar over real subprocess stdio (no AWS needed)."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dynamodb_streams_consumer import Record, Worker, decode_attr  # noqa: E402
from dynamodb_streams_consumer.record import decode_item  # noqa: E402

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
        self.lost = []
        self.requested = []

    def process_records(self, records):
        self.records.extend(records)

    def shard_ended(self, shard_id):
        self.ended.append(shard_id)

    def lease_lost(self, shard_id):
        self.lost.append(shard_id)

    def shutdown_requested(self, shard_id):
        self.requested.append(shard_id)


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
        # shard_complete over the wire invoked the optional shard_ended callback,
        # and the malformed line between batches was ignored (no crash, all acks).
        self.assertEqual(proc.ended, ["s1"])
        # lease_lost over the wire invoked the optional lease_lost callback with
        # the shard id, and did NOT emit a checkpoint (the fake would exit 3).
        self.assertEqual(proc.lost, ["s0"])
        # shutdown_requested over the wire invoked the optional callback with the
        # shard id, and did NOT emit a checkpoint (the fake would exit 3).
        self.assertEqual(proc.requested, ["s1"])


class _MinimalProcessor:
    """Only process_records — no shard_ended. Must not crash on shard_complete."""

    def __init__(self):
        self.count = 0

    def process_records(self, records):
        self.count += len(records)


class TestWorkerEdgeCases(unittest.TestCase):
    def test_processor_without_shard_ended_is_fine(self):
        proc = _MinimalProcessor()
        exit_code = Worker(
            stream_arn="arn", lease_table="l", processor=proc,
            sidecar_cmd=[sys.executable, FAKE],
        ).run()
        self.assertEqual(exit_code, 0)
        self.assertEqual(proc.count, 3)

    def test_env_maps_all_config(self):
        w = Worker(
            stream_arn="the-arn", lease_table="the-table", processor=_MinimalProcessor(),
            owner="own", region="eu-west-1", max_leases=7,
            lease_duration_ms=1234, poll_interval_ms=55, cycle_interval_ms=66,
            max_processing_concurrency=4,
            initial_position="latest",
            sidecar_cmd=["true"],
        )
        env = w._env()
        self.assertEqual(env["DDB_STREAMS_CONSUMER_STREAM_ARN"], "the-arn")
        self.assertEqual(env["DDB_STREAMS_CONSUMER_LEASE_TABLE"], "the-table")
        self.assertEqual(env["DDB_STREAMS_CONSUMER_OWNER"], "own")
        self.assertEqual(env["AWS_REGION"], "eu-west-1")
        self.assertEqual(env["DDB_STREAMS_CONSUMER_MAX_LEASES"], "7")
        self.assertEqual(env["DDB_STREAMS_CONSUMER_LEASE_DURATION_MS"], "1234")
        self.assertEqual(env["DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS"], "55")
        self.assertEqual(env["DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS"], "66")
        self.assertEqual(env["DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY"], "4")
        # case-insensitive input is normalized to uppercase
        self.assertEqual(env["DDB_STREAMS_CONSUMER_INITIAL_POSITION"], "LATEST")

    def test_env_omits_initial_position_when_unset(self):
        w = Worker(
            stream_arn="the-arn", lease_table="the-table", processor=_MinimalProcessor(),
            sidecar_cmd=["true"],
        )
        self.assertNotIn("DDB_STREAMS_CONSUMER_INITIAL_POSITION", w._env())

    def test_missing_sidecar_binary_raises(self):
        import os as _os
        from dynamodb_streams_consumer import worker as worker_mod

        saved = _os.environ.pop("DDB_STREAMS_CONSUMER_SIDECAR", None)
        saved_path = _os.environ.get("PATH")
        saved_bundled = worker_mod._bundled_sidecar
        try:
            _os.environ["PATH"] = ""  # nothing discoverable
            worker_mod._bundled_sidecar = lambda: None  # and no bundled binary
            with self.assertRaises(FileNotFoundError):
                worker_mod._discover_sidecar()
        finally:
            worker_mod._bundled_sidecar = saved_bundled
            if saved is not None:
                _os.environ["DDB_STREAMS_CONSUMER_SIDECAR"] = saved
            if saved_path is not None:
                _os.environ["PATH"] = saved_path


if __name__ == "__main__":
    unittest.main()
