package ddbstreams

import (
	"encoding/json"
	"fmt"
)

// Record is one item-level change delivered from a DynamoDB stream. Attribute
// values are decoded to Go natives: S/N -> string (numbers stay canonical
// strings, as DynamoDB carries them), Bool -> bool, Null -> nil, B -> []byte,
// M -> map[string]any, L -> []any, Ss/Ns -> []string, Bs -> [][]byte.
type Record struct {
	ShardID        string
	EventName      string // INSERT / MODIFY / REMOVE
	SequenceNumber string
	StreamViewType string // KEYS_ONLY / NEW_IMAGE / OLD_IMAGE / NEW_AND_OLD_IMAGES
	Keys           map[string]any
	NewImage       map[string]any // nil if absent
	OldImage       map[string]any // nil if absent
}

// wireRecord mirrors StreamRecord on the wire (protocol/src/lib.rs).
type wireRecord struct {
	EventName      *string                    `json:"event_name"`
	SequenceNumber *string                    `json:"sequence_number"`
	StreamViewType *string                    `json:"stream_view_type"`
	Keys           map[string]json.RawMessage `json:"keys"`
	NewImage       map[string]json.RawMessage `json:"new_image"`
	OldImage       map[string]json.RawMessage `json:"old_image"`
}

func recordFromWire(shard string, w wireRecord) (Record, error) {
	keys, err := decodeItem(w.Keys)
	if err != nil {
		return Record{}, err
	}
	ni, err := decodeItem(w.NewImage)
	if err != nil {
		return Record{}, err
	}
	oi, err := decodeItem(w.OldImage)
	if err != nil {
		return Record{}, err
	}
	r := Record{ShardID: shard, Keys: keys}
	if w.EventName != nil {
		r.EventName = *w.EventName
	}
	if w.SequenceNumber != nil {
		r.SequenceNumber = *w.SequenceNumber
	}
	if w.StreamViewType != nil {
		r.StreamViewType = *w.StreamViewType
	}
	if w.NewImage != nil {
		r.NewImage = ni
	}
	if w.OldImage != nil {
		r.OldImage = oi
	}
	return r, nil
}

// decodeItem decodes a map of attribute values; nil/empty -> empty map.
func decodeItem(item map[string]json.RawMessage) (map[string]any, error) {
	out := make(map[string]any, len(item))
	for k, raw := range item {
		v, err := decodeAttr(raw)
		if err != nil {
			return nil, fmt.Errorf("attr %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

// decodeAttr decodes one serde-externally-tagged AttrValue. The unit variant
// Null is the bare JSON string "Null"; every other variant is a single-key
// object like {"S":"x"} or {"N":"42"}.
func decodeAttr(raw json.RawMessage) (any, error) {
	// Null is encoded as the bare string "Null".
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if s == "Null" {
			return nil, nil
		}
		return nil, fmt.Errorf("unexpected bare string attribute %q", s)
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("attribute is neither Null nor a tagged object: %w", err)
	}
	if len(obj) != 1 {
		return nil, fmt.Errorf("attribute must have exactly one type tag, got %d", len(obj))
	}
	var tag string
	var val json.RawMessage
	for tag, val = range obj {
	}

	switch tag {
	case "S", "N":
		var str string
		if err := json.Unmarshal(val, &str); err != nil {
			return nil, err
		}
		return str, nil
	case "Bool":
		var b bool
		if err := json.Unmarshal(val, &b); err != nil {
			return nil, err
		}
		return b, nil
	case "B":
		return decodeBytes(val)
	case "Ss", "Ns":
		var ss []string
		if err := json.Unmarshal(val, &ss); err != nil {
			return nil, err
		}
		return ss, nil
	case "Bs":
		var arrs []json.RawMessage
		if err := json.Unmarshal(val, &arrs); err != nil {
			return nil, err
		}
		out := make([][]byte, len(arrs))
		for i, a := range arrs {
			b, err := decodeBytes(a)
			if err != nil {
				return nil, err
			}
			out[i] = b
		}
		return out, nil
	case "M":
		var m map[string]json.RawMessage
		if err := json.Unmarshal(val, &m); err != nil {
			return nil, err
		}
		return decodeItem(m)
	case "L":
		var arr []json.RawMessage
		if err := json.Unmarshal(val, &arr); err != nil {
			return nil, err
		}
		out := make([]any, len(arr))
		for i, e := range arr {
			v, err := decodeAttr(e)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown attribute type tag %q", tag)
	}
}

// decodeBytes decodes a serde Vec<u8>, which serializes as a JSON array of
// numbers (e.g. [0,1,255]) rather than base64.
func decodeBytes(val json.RawMessage) ([]byte, error) {
	var nums []int
	if err := json.Unmarshal(val, &nums); err != nil {
		return nil, err
	}
	out := make([]byte, len(nums))
	for i, n := range nums {
		if n < 0 || n > 255 {
			return nil, fmt.Errorf("byte out of range: %d", n)
		}
		out[i] = byte(n)
	}
	return out, nil
}
