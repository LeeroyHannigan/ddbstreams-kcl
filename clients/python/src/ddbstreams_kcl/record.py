"""Typed DynamoDB Streams change record, decoded from the sidecar wire format.

The sidecar serializes the Rust ``AttrValue`` enum externally-tagged, e.g.
``{"S": "k1"}``, ``{"N": "42"}``, ``{"Bool": true}``, ``"Null"``,
``{"M": {...}}``, ``{"L": [...]}``, ``{"Ss": [...]}``. :func:`decode_attr`
converts that into native Python; numbers stay as strings exactly as DynamoDB
represents them (lossless, no float rounding)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def decode_attr(av: Any) -> Any:
    """Decode one wire attribute value into a native Python value."""
    if av == "Null":
        return None
    if not isinstance(av, dict) or len(av) != 1:
        # Be permissive: unknown shapes pass through unchanged.
        return av
    tag, val = next(iter(av.items()))
    if tag == "S":
        return val
    if tag == "N":
        return val  # keep DynamoDB's canonical string form (lossless)
    if tag == "Bool":
        return val
    if tag == "B":
        # Rust serializes bytes as a JSON array of u8.
        return bytes(val)
    if tag == "M":
        return {k: decode_attr(v) for k, v in val.items()}
    if tag == "L":
        return [decode_attr(v) for v in val]
    if tag == "Ss":
        return list(val)
    if tag == "Ns":
        return list(val)  # numeric set members stay strings
    if tag == "Bs":
        return [bytes(b) for b in val]
    return av


def decode_item(item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not item:
        return {}
    return {k: decode_attr(v) for k, v in item.items()}


@dataclass
class Record:
    """One item-level change delivered from a DynamoDB stream shard."""

    shard_id: str
    sequence_number: Optional[str]
    event_name: Optional[str]  # INSERT / MODIFY / REMOVE
    stream_view_type: Optional[str]
    keys: Dict[str, Any] = field(default_factory=dict)
    new_image: Optional[Dict[str, Any]] = None
    old_image: Optional[Dict[str, Any]] = None

    @classmethod
    def from_wire(cls, shard_id: str, wire: Dict[str, Any]) -> "Record":
        ni = wire.get("new_image")
        oi = wire.get("old_image")
        return cls(
            shard_id=shard_id,
            sequence_number=wire.get("sequence_number"),
            event_name=wire.get("event_name"),
            stream_view_type=wire.get("stream_view_type"),
            keys=decode_item(wire.get("keys")),
            new_image=decode_item(ni) if ni else None,
            old_image=decode_item(oi) if oi else None,
        )
