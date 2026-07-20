"""The customer-facing entry point: spawn the amazon-dynamodb-streams-consumer sidecar and deliver
ordered, checkpointed change records to a processor.

Example::

    from dynamodb_streams_consumer import Worker

    class MyProcessor:
        def process_records(self, records):
            for r in records:
                print(r.event_name, r.keys, r.new_image)
        # optional:
        def shard_ended(self, shard_id): ...
        def lease_lost(self, shard_id): ...
        def shutdown_requested(self, shard_id): ...

    Worker(
        stream_arn="arn:aws:dynamodb:...:table/Orders/stream/2026-...",
        lease_table="my-app-leases",
        processor=MyProcessor(),
    ).run()

The sidecar owns all coordination (shard discovery, leases, ordering,
checkpoints). This client is a thin stdio bridge: it reads record batches, calls
``processor.process_records(records)``, and acks each batch so the sidecar
advances the checkpoint (at-least-once)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
from typing import Any, List, Literal, Optional, Protocol, Sequence

from .record import DDB_JSON, NATIVE, Record

#: Where a freshly-seeded shard (no checkpoint) begins reading.
TRIM_HORIZON = "TRIM_HORIZON"
LATEST = "LATEST"
InitialPosition = Literal["TRIM_HORIZON", "LATEST"]

DEFAULT_BINARY = "amazon-dynamodb-streams-consumer-sidecar"


class RecordProcessor(Protocol):
    def process_records(self, records: List[Record]) -> None: ...


def _bundled_sidecar() -> Optional[str]:
    """The sidecar binary shipped inside the installed (platform) wheel, if any."""
    bin_dir = os.path.join(os.path.dirname(__file__), "_bin")
    for name in (DEFAULT_BINARY, DEFAULT_BINARY + ".exe"):
        path = os.path.join(bin_dir, name)
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return None


def _discover_sidecar() -> str:
    # 1) explicit override, 2) bundled binary from the wheel, 3) PATH.
    env = os.environ.get("DDB_STREAMS_CONSUMER_SIDECAR")
    if env:
        return env
    bundled = _bundled_sidecar()
    if bundled:
        return bundled
    found = shutil.which(DEFAULT_BINARY)
    if found:
        return found
    raise FileNotFoundError(
        f"could not find the '{DEFAULT_BINARY}' sidecar binary. Put it on PATH, "
        "set DDB_STREAMS_CONSUMER_SIDECAR=/path/to/sidecar, or pass sidecar_path=..."
    )


class Worker:
    def __init__(
        self,
        stream_arn: str,
        lease_table: str,
        processor: RecordProcessor,
        *,
        owner: Optional[str] = None,
        region: Optional[str] = None,
        record_format: str = NATIVE,
        max_leases: Optional[int] = None,
        lease_duration_ms: Optional[int] = None,
        poll_interval_ms: Optional[int] = None,
        cycle_interval_ms: Optional[int] = None,
        max_processing_concurrency: Optional[int] = None,
        initial_position: Optional[InitialPosition] = None,
        sidecar_path: Optional[str] = None,
        sidecar_cmd: Optional[Sequence[str]] = None,
    ) -> None:
        self.stream_arn = stream_arn
        self.lease_table = lease_table
        self.processor = processor
        self.owner = owner
        self.region = region
        if record_format not in (NATIVE, DDB_JSON):
            raise ValueError(
                f"record_format must be {NATIVE!r} or {DDB_JSON!r}, got {record_format!r}"
            )
        self.record_format = record_format
        self.max_leases = max_leases
        self.lease_duration_ms = lease_duration_ms
        self.poll_interval_ms = poll_interval_ms
        self.cycle_interval_ms = cycle_interval_ms
        self.max_processing_concurrency = max_processing_concurrency
        self.initial_position = initial_position
        # sidecar_cmd overrides everything (tests / custom launch); otherwise the
        # resolved single binary.
        self._cmd = list(sidecar_cmd) if sidecar_cmd else [sidecar_path or _discover_sidecar()]
        self._proc: Optional[subprocess.Popen[str]] = None
        self._stdin_lock = threading.Lock()

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["DDB_STREAMS_CONSUMER_STREAM_ARN"] = self.stream_arn
        env["DDB_STREAMS_CONSUMER_LEASE_TABLE"] = self.lease_table
        if self.owner:
            env["DDB_STREAMS_CONSUMER_OWNER"] = self.owner
        if self.region:
            env["AWS_REGION"] = self.region
        for key, val in [
            ("DDB_STREAMS_CONSUMER_MAX_LEASES", self.max_leases),
            ("DDB_STREAMS_CONSUMER_LEASE_DURATION_MS", self.lease_duration_ms),
            ("DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS", self.poll_interval_ms),
            ("DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS", self.cycle_interval_ms),
            ("DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY", self.max_processing_concurrency),
        ]:
            if val is not None:
                env[key] = str(val)
        if self.initial_position is not None:
            env["DDB_STREAMS_CONSUMER_INITIAL_POSITION"] = str(self.initial_position).strip().upper()
        return env

    def _send(self, msg: dict[str, Any]) -> None:
        assert self._proc and self._proc.stdin
        with self._stdin_lock:
            if self._proc.stdin.closed:
                return
            self._proc.stdin.write(json.dumps(msg) + "\n")
            self._proc.stdin.flush()

    def stop(self) -> None:
        """Request a graceful stop from another thread. The sidecar finishes its
        current cycle, emits ``shutdown``, and :meth:`run` returns."""
        proc = self._proc
        if not proc or not proc.stdin:
            return
        try:
            self._send({"type": "stop"})
        except (BrokenPipeError, ValueError):
            pass

    def run(self) -> int:
        """Run until the sidecar shuts down (all shards complete, stop, or the
        process exits). Returns the sidecar's exit code. Ctrl-C triggers a
        graceful stop."""
        self._proc = subprocess.Popen(
            self._cmd,
            env=self._env(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,  # inherit: sidecar logs to our stderr
            text=True,
            bufsize=1,
        )
        proc = self._proc
        assert proc.stdout is not None
        try:
            self._send({"type": "ready"})
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue  # ignore any non-protocol noise
                self._handle(msg)
                if msg.get("type") == "shutdown":
                    break
        except KeyboardInterrupt:
            pass
        finally:
            self._stop()
        return proc.wait()

    def _handle(self, msg: dict[str, Any]) -> None:
        kind = msg.get("type")
        if kind == "records":
            shard = msg["shard"]
            records = [Record.from_wire(shard, r, self.record_format) for r in msg.get("records", [])]
            self.processor.process_records(records)
            # Ack: durably processed up to last_seq → sidecar checkpoints it.
            self._send({"type": "checkpoint", "shard": shard, "seq": msg["last_seq"]})
        elif kind == "shard_complete":
            ended = getattr(self.processor, "shard_ended", None)
            if callable(ended):
                ended(msg["shard"])
        elif kind == "lease_lost":
            # Lease stolen or expired: notify the processor but do NOT checkpoint —
            # the lease is no longer held by this worker.
            lost = getattr(self.processor, "lease_lost", None)
            if callable(lost):
                lost(msg["shard"])
        elif kind == "shutdown_requested":
            # Sidecar asked us to wind down this shard: notify the processor but do
            # NOT checkpoint — this is only a signal, not a processed position.
            requested = getattr(self.processor, "shutdown_requested", None)
            if callable(requested):
                requested(msg["shard"])
        # "shutdown" handled by the caller loop.

    def _stop(self) -> None:
        proc = self._proc
        if not proc:
            return
        try:
            if proc.stdin and not proc.stdin.closed:
                try:
                    self._send({"type": "stop"})
                except (BrokenPipeError, ValueError):
                    pass
                with self._stdin_lock:
                    if not proc.stdin.closed:
                        proc.stdin.close()
        finally:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
            for stream in (proc.stdout, proc.stderr):
                if stream and not stream.closed:
                    stream.close()
