'use strict';

// Decodes serde-externally-tagged AttrValue (protocol/src/lib.rs) to JS natives:
// S/N -> string (numbers stay canonical strings), Bool -> boolean, Null -> null,
// B -> Buffer, M -> object, L -> array, Ss/Ns -> string[], Bs -> Buffer[].
// The unit variant Null is the bare JSON string "Null"; every other variant is a
// single-key object like {"S":"x"} or {"N":"42"}.
function decodeAttr(v) {
  if (v === 'Null') return null;
  if (typeof v !== 'object' || v === null || Array.isArray(v)) {
    throw new Error(`invalid attribute value: ${JSON.stringify(v)}`);
  }
  const tags = Object.keys(v);
  if (tags.length !== 1) {
    throw new Error(`attribute must have exactly one type tag, got ${tags.length}`);
  }
  const tag = tags[0];
  const val = v[tag];
  switch (tag) {
    case 'S':
    case 'N':
      return String(val);
    case 'Bool':
      return Boolean(val);
    case 'B':
      return Buffer.from(val); // val is an array of byte ints
    case 'Ss':
    case 'Ns':
      return val.map(String);
    case 'Bs':
      return val.map((a) => Buffer.from(a));
    case 'M':
      return decodeItem(val);
    case 'L':
      return val.map(decodeAttr);
    default:
      throw new Error(`unknown attribute type tag: ${tag}`);
  }
}

function decodeItem(item) {
  const out = {};
  if (!item) return out;
  for (const [k, v] of Object.entries(item)) out[k] = decodeAttr(v);
  return out;
}

function recordFromWire(shard, w) {
  return {
    shardId: shard,
    eventName: w.event_name ?? null,
    sequenceNumber: w.sequence_number ?? null,
    streamViewType: w.stream_view_type ?? null,
    keys: decodeItem(w.keys),
    newImage: w.new_image ? decodeItem(w.new_image) : null,
    oldImage: w.old_image ? decodeItem(w.old_image) : null,
  };
}

module.exports = { decodeAttr, decodeItem, recordFromWire };
