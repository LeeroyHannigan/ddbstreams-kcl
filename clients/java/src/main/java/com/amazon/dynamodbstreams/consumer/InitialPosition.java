package com.amazon.dynamodbstreams.consumer;

/**
 * Where a freshly-seeded shard (no checkpoint) begins reading. The enum
 * constant names match the values the sidecar expects on the wire, so
 * forwarding is just {@link #name()}.
 */
public enum InitialPosition {
    /** Read the shard from the oldest available record (default). */
    TRIM_HORIZON,

    /** Read only records written after the consumer starts. */
    LATEST,
}
