package com.amazon.dynamodbstreams.consumer;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Verifies how {@link WorkerConfig} options are surfaced to the sidecar environment. */
class WorkerEnvTest {

    private static Map<String, String> envFor(WorkerConfig config) throws Exception {
        Worker worker = new Worker(config);
        ProcessBuilder pb = new ProcessBuilder("true");
        Method applyEnv = Worker.class.getDeclaredMethod("applyEnv", ProcessBuilder.class);
        applyEnv.setAccessible(true);
        applyEnv.invoke(worker, pb);
        return pb.environment();
    }

    private static WorkerConfig.Builder baseBuilder() {
        return WorkerConfig.builder()
                .streamArn("arn:aws:dynamodb:us-east-1:123456789012:table/T/stream/x")
                .leaseTable("leases")
                .processor(records -> {
                });
    }

    @Test
    void initialPositionIsNormalizedToUpperCase() throws Exception {
        Map<String, String> env = envFor(baseBuilder().initialPosition(InitialPosition.LATEST).build());
        assertEquals("LATEST", env.get("DDB_STREAMS_CONSUMER_INITIAL_POSITION"));
    }

    @Test
    void initialPositionAbsentWhenUnset() throws Exception {
        Map<String, String> env = envFor(baseBuilder().build());
        assertFalse(env.containsKey("DDB_STREAMS_CONSUMER_INITIAL_POSITION"));
    }
}
