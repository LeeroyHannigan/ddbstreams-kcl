#!/usr/bin/env python3
"""A fake sidecar for hermetic client tests. Speaks the same JSON-Lines protocol
as the real Rust sidecar: emits two record batches, then verifies the client's
checkpoint acks carry each batch's last_seq. Exits 0 on correct acks, 3 on a
mismatch — so a test can assert the ack path end-to-end without AWS."""

import json
import sys


def emit(msg):
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def rec(seq, pk, active):
    return {
        "event_name": "INSERT",
        "sequence_number": seq,
        "size_bytes": 10,
        "stream_view_type": "NEW_AND_OLD_IMAGES",
        "keys": {"pk": {"S": pk}},
        "new_image": {"pk": {"S": pk}, "active": {"Bool": active}, "n": {"N": "42"}},
        "old_image": None,
    }


def main():
    emit({"type": "records", "shard": "s0", "last_seq": "2",
          "records": [rec("1", "k1", True), rec("2", "k2", False)]})
    emit({"type": "records", "shard": "s1", "last_seq": "9",
          "records": [rec("9", "k9", True)]})

    expected = {"s0": "2", "s1": "9"}
    got = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        msg = json.loads(line)
        if msg.get("type") == "checkpoint":
            if msg.get("seq") != expected.get(msg.get("shard")):
                sys.exit(3)  # wrong ack
            got += 1
            if got == len(expected):
                break
        elif msg.get("type") == "stop":
            break

    emit({"type": "shutdown", "reason": "done"})
    sys.exit(0)


if __name__ == "__main__":
    main()
