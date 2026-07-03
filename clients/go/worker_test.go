package ddbstreams

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

// Drives every shared conformance fixture (conformance/fixtures/*.json) through
// the real Worker against the shared replay_sidecar.py -- no AWS, no real
// sidecar. Mirrors clients/python/tests/test_conformance.py.

type collector struct {
	byShard map[string][]string
	ended   []string
}

func newCollector() *collector { return &collector{byShard: map[string][]string{}} }

func (c *collector) ProcessRecords(records []Record) {
	for _, r := range records {
		c.byShard[r.ShardID] = append(c.byShard[r.ShardID], r.SequenceNumber)
	}
}

func (c *collector) ShardEnded(shardID string) { c.ended = append(c.ended, shardID) }

type fixtureExpect struct {
	RecordsPerShard map[string]int      `json:"records_per_shard"`
	RecordOrder     map[string][]string `json:"record_order"`
	ShardEnded      []string            `json:"shard_ended"`
}

type fixture struct {
	Name   string        `json:"name"`
	Expect fixtureExpect `json:"expect"`
}

func TestConformance(t *testing.T) {
	confDir := filepath.Join("..", "..", "conformance")
	replay := filepath.Join(confDir, "replay_sidecar.py")
	fixtures, err := filepath.Glob(filepath.Join(confDir, "fixtures", "*.json"))
	if err != nil || len(fixtures) == 0 {
		t.Fatalf("no fixtures under %s/fixtures (err=%v)", confDir, err)
	}
	python := pythonBin(t)

	for _, fx := range fixtures {
		fx := fx
		raw, err := os.ReadFile(fx)
		if err != nil {
			t.Fatalf("read %s: %v", fx, err)
		}
		var f fixture
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("parse %s: %v", fx, err)
		}
		t.Run(f.Name, func(t *testing.T) {
			c := newCollector()
			w, err := New(Config{
				StreamArn:  "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026",
				LeaseTable: "leases",
				Processor:  c,
				SidecarCmd: []string{python, replay, fx},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			code, err := w.Run()
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			// Checkpointing: replay exits non-zero on a wrong/absent ack.
			if code != 0 {
				t.Fatalf("%s: replay rejected checkpoint acks (exit %d)", f.Name, code)
			}
			// Delivery: counts + per-shard order.
			for shard, want := range f.Expect.RecordsPerShard {
				if got := len(c.byShard[shard]); got != want {
					t.Errorf("%s: shard %s count = %d, want %d", f.Name, shard, got, want)
				}
			}
			if len(c.byShard) != len(f.Expect.RecordsPerShard) {
				t.Errorf("%s: delivered shards = %v, want keys %v", f.Name,
					keys(c.byShard), keysInt(f.Expect.RecordsPerShard))
			}
			for shard, order := range f.Expect.RecordOrder {
				if !equalStr(c.byShard[shard], order) {
					t.Errorf("%s: shard %s order = %v, want %v", f.Name, shard, c.byShard[shard], order)
				}
			}
			// Lifecycle: shard_ended.
			got := append([]string(nil), c.ended...)
			want := append([]string(nil), f.Expect.ShardEnded...)
			sort.Strings(got)
			sort.Strings(want)
			if !equalStr(got, want) {
				t.Errorf("%s: shard_ended = %v, want %v", f.Name, got, want)
			}
		})
	}
}

func pythonBin(t *testing.T) string {
	for _, name := range []string{"python3", "python"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	t.Skip("python3 not found; conformance replay sidecar requires it")
	return ""
}

func equalStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func keys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keysInt(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
