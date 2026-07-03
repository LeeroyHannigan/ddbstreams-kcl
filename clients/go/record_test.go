package ddbstreams

import (
	"encoding/json"
	"reflect"
	"testing"
)

func dec(t *testing.T, s string) any {
	t.Helper()
	v, err := decodeAttr(json.RawMessage(s))
	if err != nil {
		t.Fatalf("decodeAttr(%s): %v", s, err)
	}
	return v
}

func TestDecodeScalars(t *testing.T) {
	if got := dec(t, `{"S":"hi"}`); got != "hi" {
		t.Errorf("S = %v", got)
	}
	if got := dec(t, `{"N":"42"}`); got != "42" { // numbers stay canonical strings
		t.Errorf("N = %v", got)
	}
	if got := dec(t, `{"Bool":true}`); got != true {
		t.Errorf("Bool = %v", got)
	}
	if got := dec(t, `"Null"`); got != nil {
		t.Errorf("Null = %v", got)
	}
}

func TestDecodeBinaryAndSets(t *testing.T) {
	if got := dec(t, `{"B":[0,1,255]}`); !reflect.DeepEqual(got, []byte{0, 1, 255}) {
		t.Errorf("B = %v", got)
	}
	if got := dec(t, `{"Ss":["a","b"]}`); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("Ss = %v", got)
	}
	if got := dec(t, `{"Ns":["1","2.5"]}`); !reflect.DeepEqual(got, []string{"1", "2.5"}) {
		t.Errorf("Ns = %v", got)
	}
	if got := dec(t, `{"Bs":[[1,2],[3]]}`); !reflect.DeepEqual(got, [][]byte{{1, 2}, {3}}) {
		t.Errorf("Bs = %v", got)
	}
}

func TestDecodeNested(t *testing.T) {
	got := dec(t, `{"M":{"x":{"N":"1"},"y":{"S":"z"},"n":"Null"}}`)
	want := map[string]any{"x": "1", "y": "z", "n": nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("M = %v, want %v", got, want)
	}
	gotL := dec(t, `{"L":[{"S":"a"},"Null",{"Bool":false}]}`)
	wantL := []any{"a", nil, false}
	if !reflect.DeepEqual(gotL, wantL) {
		t.Errorf("L = %v, want %v", gotL, wantL)
	}
}

func TestRecordFromWire(t *testing.T) {
	line := `{"event_name":"MODIFY","sequence_number":"100","stream_view_type":"NEW_AND_OLD_IMAGES",` +
		`"keys":{"pk":{"S":"k1"}},"new_image":{"pk":{"S":"k1"},"active":{"Bool":true}},"old_image":null}`
	var w wireRecord
	if err := json.Unmarshal([]byte(line), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	r, err := recordFromWire("shardId-1", w)
	if err != nil {
		t.Fatalf("recordFromWire: %v", err)
	}
	if r.ShardID != "shardId-1" || r.EventName != "MODIFY" || r.SequenceNumber != "100" {
		t.Errorf("scalars: %+v", r)
	}
	if !reflect.DeepEqual(r.Keys, map[string]any{"pk": "k1"}) {
		t.Errorf("keys = %v", r.Keys)
	}
	if !reflect.DeepEqual(r.NewImage, map[string]any{"pk": "k1", "active": true}) {
		t.Errorf("new_image = %v", r.NewImage)
	}
	if r.OldImage != nil {
		t.Errorf("old_image = %v, want nil", r.OldImage)
	}
}
