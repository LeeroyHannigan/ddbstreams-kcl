// Package ddbstreams is a JVM-free DynamoDB Streams consumer for Go. It embeds
// the shared Rust sidecar (which owns shard discovery, leasing, ordering, and
// checkpointing) and delivers ordered, checkpointed change records to a
// processor. It is the Go analog of the Python client: a thin stdio bridge over
// the JSON-Lines wire protocol (see protocol/src/lib.rs).
package ddbstreams

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

// Version is the binding version; it also selects the sidecar release to fetch.
const Version = "0.1.0"

// RecordProcessor receives batches of ordered records for a shard.
type RecordProcessor interface {
	ProcessRecords(records []Record)
}

// ShardEndedHandler is optionally implemented by a processor to be notified
// when a shard reaches SHARD_END.
type ShardEndedHandler interface {
	ShardEnded(shardID string)
}

// LeaseLostHandler is optionally implemented by a processor to be notified
// when this worker loses a shard's lease (another worker now owns the shard).
// Do NOT checkpoint from this callback: the shard is no longer owned by this
// worker.
type LeaseLostHandler interface {
	LeaseLost(shardID string)
}

// ShutdownRequestedHandler is optionally implemented by a processor to be
// notified when the sidecar begins a graceful shutdown for a shard it still
// owns. It is called once per owned shard before the lease is handed off, so
// the processor can flush any buffered work. Do NOT checkpoint from this
// callback: the lease handoff is imminent and the sidecar controls the final
// checkpoint.
type ShutdownRequestedHandler interface {
	ShutdownRequested(shardID string)
}

// InitialPosition selects where a freshly-seeded shard (no checkpoint) begins
// reading. Use the constants below; a bare string ("TRIM_HORIZON"/"LATEST")
// also assigns since the underlying type is string.
type InitialPosition string

const (
	TrimHorizon InitialPosition = "TRIM_HORIZON"
	Latest      InitialPosition = "LATEST"
)

// Config configures a Worker. StreamArn, LeaseTable, and Processor are required.
type Config struct {
	StreamArn  string
	LeaseTable string
	Processor  RecordProcessor
	Owner      string
	Region     string
	// RecordFormat selects how attribute values are exposed (default "native").
	// Set to RecordFormatDDBJSON for canonical DynamoDB JSON (SDK interop).
	RecordFormat    RecordFormat
	MaxLeases       int
	LeaseDurationMS int
	PollIntervalMS  int
	CycleIntervalMS int
	// MaxProcessingConcurrency caps the shards processed concurrently (opt-in;
	// 0 = unbounded, one slot per shard). Bounds concurrent delivery so footprint
	// stays O(max) as shard count grows; preserves at-least-once + per-item +
	// per-shard ordering.
	MaxProcessingConcurrency int
	// InitialPosition selects where a freshly-seeded shard begins reading:
	// TrimHorizon (default) or Latest.
	InitialPosition InitialPosition

	// SidecarPath overrides sidecar discovery with an explicit binary path.
	SidecarPath string
	// SidecarCmd overrides the launch command entirely (tests / custom launch).
	SidecarCmd []string
}

// Worker runs the consumer until the sidecar shuts down.
type Worker struct {
	cfg       Config
	cmd       *exec.Cmd
	stdin     *bufio.Writer
	stdinPipe interface{ Close() error }
	mu        sync.Mutex
	closed    bool
}

// New builds a Worker. It resolves the sidecar command up front so a missing
// binary is reported before Run.
func New(cfg Config) (*Worker, error) {
	if cfg.StreamArn == "" || cfg.LeaseTable == "" || cfg.Processor == nil {
		return nil, fmt.Errorf("StreamArn, LeaseTable and Processor are required")
	}
	return &Worker{cfg: cfg}, nil
}

func (w *Worker) command() ([]string, error) {
	if len(w.cfg.SidecarCmd) > 0 {
		return w.cfg.SidecarCmd, nil
	}
	bin, err := discoverSidecar(w.cfg.SidecarPath)
	if err != nil {
		return nil, err
	}
	return []string{bin}, nil
}

func (w *Worker) env() []string {
	env := os.Environ()
	add := func(k, v string) { env = append(env, k+"="+v) }
	add("DDB_STREAMS_CONSUMER_STREAM_ARN", w.cfg.StreamArn)
	add("DDB_STREAMS_CONSUMER_LEASE_TABLE", w.cfg.LeaseTable)
	if w.cfg.Owner != "" {
		add("DDB_STREAMS_CONSUMER_OWNER", w.cfg.Owner)
	}
	if w.cfg.Region != "" {
		add("AWS_REGION", w.cfg.Region)
	}
	if w.cfg.MaxLeases > 0 {
		add("DDB_STREAMS_CONSUMER_MAX_LEASES", strconv.Itoa(w.cfg.MaxLeases))
	}
	if w.cfg.MaxProcessingConcurrency > 0 {
		add("DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY", strconv.Itoa(w.cfg.MaxProcessingConcurrency))
	}
	if w.cfg.LeaseDurationMS > 0 {
		add("DDB_STREAMS_CONSUMER_LEASE_DURATION_MS", strconv.Itoa(w.cfg.LeaseDurationMS))
	}
	if w.cfg.PollIntervalMS > 0 {
		add("DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS", strconv.Itoa(w.cfg.PollIntervalMS))
	}
	if w.cfg.CycleIntervalMS > 0 {
		add("DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS", strconv.Itoa(w.cfg.CycleIntervalMS))
	}
	if w.cfg.InitialPosition != "" {
		add("DDB_STREAMS_CONSUMER_INITIAL_POSITION", strings.ToUpper(strings.TrimSpace(string(w.cfg.InitialPosition))))
	}
	return env
}

func (w *Worker) send(msg map[string]any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.stdin == nil {
		return
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return
	}
	if _, err := w.stdin.Write(append(b, '\n')); err != nil {
		return
	}
	_ = w.stdin.Flush()
}

// Stop requests a graceful shutdown from another goroutine. The sidecar
// finishes its current cycle, emits shutdown, and Run returns.
func (w *Worker) Stop() {
	w.send(map[string]any{"type": "stop"})
}

// Run launches the sidecar and processes records until it shuts down. Returns
// the sidecar's exit code.
func (w *Worker) Run() (int, error) {
	argv, err := w.command()
	if err != nil {
		return -1, err
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = w.env()
	cmd.Stderr = os.Stderr // sidecar logs to our stderr

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return -1, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return -1, err
	}
	if err := cmd.Start(); err != nil {
		return -1, err
	}
	w.mu.Lock()
	w.cmd = cmd
	w.stdin = bufio.NewWriter(stdinPipe)
	w.stdinPipe = stdinPipe
	w.mu.Unlock()

	// Handshake.
	w.send(map[string]any{"type": "ready"})

	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // allow large record batches
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg struct {
			Type    string       `json:"type"`
			Shard   string       `json:"shard"`
			LastSeq string       `json:"last_seq"`
			Records []wireRecord `json:"records"`
		}
		if err := json.Unmarshal(line, &msg); err != nil {
			continue // ignore malformed / non-protocol noise
		}
		switch msg.Type {
		case "records":
			recs := make([]Record, 0, len(msg.Records))
			for _, wr := range msg.Records {
				r, err := recordFromWire(msg.Shard, wr, w.cfg.RecordFormat)
				if err != nil {
					continue // skip an undecodable record rather than crash
				}
				recs = append(recs, r)
			}
			w.cfg.Processor.ProcessRecords(recs)
			// Ack: durably processed up to last_seq -> sidecar checkpoints it.
			w.send(map[string]any{"type": "checkpoint", "shard": msg.Shard, "seq": msg.LastSeq})
		case "shard_complete":
			if h, ok := w.cfg.Processor.(ShardEndedHandler); ok {
				h.ShardEnded(msg.Shard)
			}
		case "lease_lost":
			// This worker lost the shard's lease; another worker now owns it.
			// Do not checkpoint -- we no longer own the shard.
			if h, ok := w.cfg.Processor.(LeaseLostHandler); ok {
				h.LeaseLost(msg.Shard)
			}
		case "shutdown_requested":
			// Graceful shutdown in progress; called once per owned shard before
			// the lease is handed off so the processor can flush. Do not
			// checkpoint -- the lease handoff is imminent.
			if h, ok := w.cfg.Processor.(ShutdownRequestedHandler); ok {
				h.ShutdownRequested(msg.Shard)
			}
		case "shutdown":
			w.stop()
			return w.wait(), nil
		}
	}
	w.stop()
	return w.wait(), scanner.Err()
}

func (w *Worker) stop() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	if w.stdin != nil {
		_ = w.stdin.Flush()
	}
	pipe := w.stdinPipe
	w.mu.Unlock()
	if pipe != nil {
		_ = pipe.Close()
	}
}

func (w *Worker) wait() int {
	if w.cmd == nil {
		return -1
	}
	err := w.cmd.Wait()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if e, ok := err.(*exec.ExitError); ok {
		exitErr = e
		return exitErr.ExitCode()
	}
	return -1
}
