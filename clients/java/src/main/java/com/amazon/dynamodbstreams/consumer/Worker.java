package com.amazon.dynamodbstreams.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A JVM-free-core DynamoDB Streams consumer. Spawns the shared Rust sidecar and
 * delivers ordered, checkpointed change records to a {@link RecordProcessor}
 * over the JSON-Lines wire protocol. The coordination logic (shard discovery,
 * leasing, ordering, checkpointing) runs in the native sidecar, not the JVM.
 */
public final class Worker {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WorkerConfig config;
    private volatile Process proc;
    private volatile boolean closed;

    public Worker(WorkerConfig config) {
        this.config = config;
    }

    /**
     * Run until the sidecar shuts down (stream fully consumed, {@link #stop()}
     * called, or fatal error). Returns the sidecar's exit code.
     */
    public int run() throws IOException, InterruptedException {
        List<String> argv = (config.sidecarCmd != null && !config.sidecarCmd.isEmpty())
                ? new ArrayList<>(config.sidecarCmd)
                : List.of(Sidecar.discover(config.sidecarPath));

        ProcessBuilder pb = new ProcessBuilder(argv);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT); // sidecar logs to our stderr
        applyEnv(pb);

        proc = pb.start();
        OutputStream stdin = proc.getOutputStream();
        BufferedReader stdout = new BufferedReader(
                new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));

        send(stdin, readyMessage());

        String line;
        while ((line = stdout.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            JsonNode msg;
            try {
                msg = MAPPER.readTree(line);
            } catch (IOException e) {
                continue; // ignore malformed / non-protocol noise
            }
            if (!msg.isObject() || !msg.hasNonNull("type")) {
                continue;
            }
            String type = msg.get("type").asText();
            switch (type) {
                case "records":
                    handleRecords(msg, stdin);
                    break;
                case "shard_complete":
                    config.processor.shardEnded(msg.path("shard").asText(""));
                    break;
                case "lease_lost":
                    config.processor.leaseLost(msg.path("shard").asText(""));
                    break;
                case "shutdown_requested":
                    config.processor.shutdownRequested(msg.path("shard").asText(""));
                    break;
                case "shutdown":
                    stopInternal(stdin);
                    break;
                default:
                    break;
            }
        }

        int code = proc.waitFor();
        closed = true;
        return code;
    }

    /** Request a graceful shutdown; {@link #run()} returns once the sidecar exits. */
    public void stop() {
        Process p = proc;
        if (p != null && !closed) {
            try {
                send(p.getOutputStream(), stopMessage());
            } catch (IOException ignored) {
                // pipe already gone
            }
        }
    }

    private void handleRecords(JsonNode root, OutputStream stdin) throws IOException {
        String shard = root.path("shard").asText("");
        String lastSeq = root.path("last_seq").asText(null);

        List<Record> records = new ArrayList<>();
        JsonNode recs = root.get("records");
        if (recs != null && recs.isArray()) {
            for (JsonNode r : recs) {
                records.add(recordFromWire(shard, r, config.recordFormat));
            }
        }

        config.processor.processRecords(records);

        ObjectNode ck = MAPPER.createObjectNode();
        ck.put("type", "checkpoint");
        ck.put("shard", shard);
        ck.put("seq", lastSeq);
        send(stdin, ck);
    }

    private static Record recordFromWire(String shard, JsonNode w, RecordFormat fmt) {
        return new Record(
                shard,
                text(w, "event_name"),
                text(w, "sequence_number"),
                text(w, "stream_view_type"),
                image(w, "keys", fmt),
                image(w, "new_image", fmt),
                image(w, "old_image", fmt));
    }

    private static String text(JsonNode w, String field) {
        JsonNode n = w.get(field);
        return (n != null && n.isTextual()) ? n.asText() : null;
    }

    private static Map<String, Object> image(JsonNode w, String field, RecordFormat fmt) {
        JsonNode n = w.get(field);
        if (n == null || !n.isObject()) {
            return null;
        }
        return fmt == RecordFormat.SDK
                ? SdkAttributeValues.decodeItem(n)
                : AttributeValues.decodeItem(n, fmt);
    }

    private void applyEnv(ProcessBuilder pb) {
        Map<String, String> e = pb.environment();
        e.put("DDB_STREAMS_CONSUMER_STREAM_ARN", config.streamArn);
        e.put("DDB_STREAMS_CONSUMER_LEASE_TABLE", config.leaseTable);
        if (config.owner != null && !config.owner.isEmpty()) {
            e.put("DDB_STREAMS_CONSUMER_OWNER", config.owner);
        }
        if (config.region != null && !config.region.isEmpty()) {
            e.put("AWS_REGION", config.region);
        }
        if (config.maxLeases != null) {
            e.put("DDB_STREAMS_CONSUMER_MAX_LEASES", config.maxLeases.toString());
        }
        if (config.maxProcessingConcurrency != null) {
            e.put("DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY", config.maxProcessingConcurrency.toString());
        }
        if (config.leaseDurationMs != null) {
            e.put("DDB_STREAMS_CONSUMER_LEASE_DURATION_MS", config.leaseDurationMs.toString());
        }
        if (config.pollIntervalMs != null) {
            e.put("DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS", config.pollIntervalMs.toString());
        }
        if (config.cycleIntervalMs != null) {
            e.put("DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS", config.cycleIntervalMs.toString());
        }
        if (config.initialPosition != null) {
            e.put("DDB_STREAMS_CONSUMER_INITIAL_POSITION", config.initialPosition.name());
        }
    }

    private void stopInternal(OutputStream stdin) {
        if (closed) {
            return;
        }
        try {
            send(stdin, stopMessage());
        } catch (IOException ignored) {
            // pipe gone
        }
        try {
            stdin.close();
        } catch (IOException ignored) {
            // already closed
        }
    }

    private static ObjectNode readyMessage() {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("type", "ready");
        return n;
    }

    private static ObjectNode stopMessage() {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("type", "stop");
        return n;
    }

    private static void send(OutputStream stdin, JsonNode msg) throws IOException {
        byte[] bytes = (MAPPER.writeValueAsString(msg) + "\n").getBytes(StandardCharsets.UTF_8);
        stdin.write(bytes);
        stdin.flush();
    }
}
