package ddbstreams

import (
	"strings"
	"testing"
)

// envMap builds a key->value map from the Worker.env() slice, keeping the last
// value for a key (matching child-process precedence).
func envMap(env []string) map[string]string {
	out := map[string]string{}
	for _, kv := range env {
		if i := strings.IndexByte(kv, '='); i >= 0 {
			out[kv[:i]] = kv[i+1:]
		}
	}
	return out
}

func TestEnvInitialPosition(t *testing.T) {
	base := Config{
		StreamArn:  "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026",
		LeaseTable: "leases",
		Processor:  newCollector(),
	}

	// Set: normalized to uppercase.
	set := base
	set.InitialPosition = "latest"
	w, err := New(set)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := envMap(w.env())["DDB_STREAMS_CONSUMER_INITIAL_POSITION"]; got != "LATEST" {
		t.Errorf("DDB_STREAMS_CONSUMER_INITIAL_POSITION = %q, want %q", got, "LATEST")
	}

	// Unset: env var absent.
	w2, err := New(base)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := envMap(w2.env())["DDB_STREAMS_CONSUMER_INITIAL_POSITION"]; ok {
		t.Errorf("DDB_STREAMS_CONSUMER_INITIAL_POSITION should be absent when unset")
	}
}
