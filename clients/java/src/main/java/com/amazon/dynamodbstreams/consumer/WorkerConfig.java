package com.amazon.dynamodbstreams.consumer;

import java.util.List;
import java.util.Objects;

/** Configuration for a {@link Worker}. Build via {@link #builder()}. */
public final class WorkerConfig {
    final String streamArn;
    final String leaseTable;
    final RecordProcessor processor;
    final String owner;
    final String region;
    final RecordFormat recordFormat;
    final Integer maxLeases;
    final Long leaseDurationMs;
    final Long pollIntervalMs;
    final Long cycleIntervalMs;
    final Integer maxProcessingConcurrency;
    final InitialPosition initialPosition;
    final String sidecarPath;
    final List<String> sidecarCmd;

    private WorkerConfig(Builder b) {
        this.streamArn = Objects.requireNonNull(b.streamArn, "streamArn is required");
        this.leaseTable = Objects.requireNonNull(b.leaseTable, "leaseTable is required");
        this.processor = Objects.requireNonNull(b.processor, "processor is required");
        this.owner = b.owner;
        this.region = b.region;
        this.recordFormat = b.recordFormat;
        this.maxLeases = b.maxLeases;
        this.leaseDurationMs = b.leaseDurationMs;
        this.pollIntervalMs = b.pollIntervalMs;
        this.cycleIntervalMs = b.cycleIntervalMs;
        this.maxProcessingConcurrency = b.maxProcessingConcurrency;
        this.initialPosition = b.initialPosition;
        this.sidecarPath = b.sidecarPath;
        this.sidecarCmd = b.sidecarCmd;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for {@link WorkerConfig}. */
    public static final class Builder {
        private String streamArn;
        private String leaseTable;
        private RecordProcessor processor;
        private String owner;
        private String region;
        private RecordFormat recordFormat = RecordFormat.NATIVE;
        private Integer maxLeases;
        private Long leaseDurationMs;
        private Long pollIntervalMs;
        private Long cycleIntervalMs;
        private Integer maxProcessingConcurrency;
        private InitialPosition initialPosition;
        private String sidecarPath;
        private List<String> sidecarCmd;

        /** The DynamoDB Streams ARN to consume (required). */
        public Builder streamArn(String v) {
            this.streamArn = v;
            return this;
        }

        /** The DynamoDB table storing shard leases + checkpoints (required). */
        public Builder leaseTable(String v) {
            this.leaseTable = v;
            return this;
        }

        /** The record processor (required). */
        public Builder processor(RecordProcessor v) {
            this.processor = v;
            return this;
        }

        /** Unique worker identity for lease ownership. Optional. */
        public Builder owner(String v) {
            this.owner = v;
            return this;
        }

        /** AWS region. Optional (falls back to the standard AWS environment). */
        public Builder region(String v) {
            this.region = v;
            return this;
        }

        /** How attribute values are surfaced. Defaults to {@link RecordFormat#NATIVE}. */
        public Builder recordFormat(RecordFormat v) {
            this.recordFormat = v;
            return this;
        }

        public Builder maxLeases(int v) {
            this.maxLeases = v;
            return this;
        }

        /** Cap on shards processed concurrently (opt-in). Unset = one slot per shard.
         *  Bounds concurrent delivery so footprint stays O(max) as shard count grows;
         *  preserves at-least-once + per-item + per-shard ordering. */
        public Builder maxProcessingConcurrency(int v) {
            this.maxProcessingConcurrency = v;
            return this;
        }

        public Builder leaseDurationMs(long v) {
            this.leaseDurationMs = v;
            return this;
        }

        public Builder pollIntervalMs(long v) {
            this.pollIntervalMs = v;
            return this;
        }

        public Builder cycleIntervalMs(long v) {
            this.cycleIntervalMs = v;
            return this;
        }

        /** Where consumption starts when no checkpoint exists: {@link InitialPosition#TRIM_HORIZON} (default) or {@link InitialPosition#LATEST}. Optional. */
        public Builder initialPosition(InitialPosition v) {
            this.initialPosition = v;
            return this;
        }

        /** Explicit sidecar binary path (overrides discovery). Optional. */
        public Builder sidecarPath(String v) {
            this.sidecarPath = v;
            return this;
        }

        /** Full launch argv (tests / custom launch; overrides discovery). Optional. */
        public Builder sidecarCmd(List<String> v) {
            this.sidecarCmd = v;
            return this;
        }

        public WorkerConfig build() {
            return new WorkerConfig(this);
        }
    }
}
