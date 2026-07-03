#!/usr/bin/env python3
"""Language-agnostic fixture-replay fake sidecar for binding conformance tests.

It speaks the exact JSON-Lines wire protocol of the real Rust sidecar (see
`protocol/src/lib.rs`) but is driven entirely by a fixture file, so every
language binding can be conformance-tested against the *same* fixtures without
AWS and without re-implementing a fake per language.

Usage (as a binding's `sidecar_cmd`):

    replay_sidecar.py <fixture.json>

The fixture's `server_script` is an ordered list of steps:

  {"emit": <object>}            write one JSON-Lines message to stdout
                                (type: records | shard_complete | shutdown)
  {"emit_raw": "<string>"}      write a raw line verbatim (used to inject a
                                malformed line the binding must ignore)
  {"await_checkpoints": [       block reading client stdin until each listed
      {"shard": "..","seq": ".."} checkpoint ack is received, validating that
   ]}                             the acked seq matches. Other client messages
                                  (ready, stop) are accepted and ignored.

Exit codes (the server-side half of the contract, asserted by the harness):
  0  script completed and every awaited checkpoint matched
  3  a checkpoint ack carried the wrong seq for its shard
  4  client stdin closed before the awaited checkpoints were satisfied
  2  bad fixture / usage

Stdlib only -- runnable under any Python 3 present in a binding's CI.
"""
import json
import sys


def _emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _emit_raw(line):
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _await_checkpoints(expected):
    # expected: list of {"shard","seq"}; consume stdin until all are seen.
    pending = {e["shard"]: e["seq"] for e in expected}
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue  # tolerate any client-side noise
        t = msg.get("type")
        if t == "checkpoint":
            shard = msg.get("shard")
            if shard in pending:
                if msg.get("seq") != pending[shard]:
                    sys.exit(3)  # wrong ack
                del pending[shard]
                if not pending:
                    return
        elif t == "stop":
            # client asked to stop before acking everything expected
            if pending:
                sys.exit(4)
            return
    # stdin exhausted before all checkpoints acked
    if pending:
        sys.exit(4)


def main():
    if len(sys.argv) != 2:
        sys.stderr.write("usage: replay_sidecar.py <fixture.json>\n")
        sys.exit(2)
    try:
        with open(sys.argv[1]) as f:
            fixture = json.load(f)
        script = fixture["server_script"]
    except (OSError, ValueError, KeyError) as e:
        sys.stderr.write(f"bad fixture: {e}\n")
        sys.exit(2)

    for step in script:
        if "emit" in step:
            _emit(step["emit"])
        elif "emit_raw" in step:
            _emit_raw(step["emit_raw"])
        elif "await_checkpoints" in step:
            _await_checkpoints(step["await_checkpoints"])
        else:
            sys.stderr.write(f"unknown step: {step}\n")
            sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
