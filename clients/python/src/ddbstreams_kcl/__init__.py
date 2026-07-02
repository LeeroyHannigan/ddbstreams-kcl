"""ddbstreams-kcl: a JVM-free DynamoDB Streams KCL consumer for Python.

The Rust sidecar owns shard discovery, DynamoDB leases, ordering and
checkpoints; this package spawns it and delivers ordered, checkpointed change
records to your :class:`RecordProcessor`."""

from .record import Record, decode_attr, decode_item
from .worker import RecordProcessor, Worker

__all__ = ["Worker", "Record", "RecordProcessor", "decode_attr", "decode_item"]
__version__ = "0.0.1"
