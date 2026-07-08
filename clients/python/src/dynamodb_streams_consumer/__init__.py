"""amazon-dynamodb-streams-consumer: a JVM-free DynamoDB Streams KCL consumer for Python.

The Rust sidecar owns shard discovery, DynamoDB leases, ordering and
checkpoints; this package spawns it and delivers ordered, checkpointed change
records to your :class:`RecordProcessor`."""

from .record import Record, decode_attr, decode_item
from .worker import LATEST, TRIM_HORIZON, InitialPosition, RecordProcessor, Worker

__all__ = [
    "Worker",
    "Record",
    "RecordProcessor",
    "decode_attr",
    "decode_item",
    "InitialPosition",
    "TRIM_HORIZON",
    "LATEST",
]
__version__ = "0.1.0"
