//! Adaptive catch-up polling backoff.
//!
//! DynamoDB Streams throttles `GetRecords` to roughly **4 calls/sec/shard**, so
//! even when a consumer is catching up it must not poll faster than ~250 ms
//! apart. When a shard is idle (empty `GetRecords` at the tip) the consumer
//! should back off to avoid burning that quota on empty reads. This mirrors the
//! intent of the adapter's `DynamoDBStreamsSleepTimeController` /
//! `PollingConfig` (awslabs/dynamodb-streams-kinesis-adapter, Apache-2.0).
//!
//! Pure and deterministic: [`PollBackoff::next_sleep_ms`] takes only whether the
//! last poll returned records and returns the milliseconds to sleep before the
//! next `GetRecords`.

/// Adaptive sleep controller for one shard's poll loop.
#[derive(Clone, Debug)]
pub struct PollBackoff {
    /// Floor between polls even while catching up (DDB Streams ~4 GetRecords/s).
    min_interval_ms: u64,
    /// Base idle sleep after the first empty poll.
    idle_base_ms: u64,
    /// Cap on the idle backoff.
    max_ms: u64,
    /// Current idle sleep, grown while the shard stays empty.
    current_idle_ms: u64,
}

impl PollBackoff {
    pub fn new(min_interval_ms: u64, idle_base_ms: u64, max_ms: u64) -> Self {
        Self {
            min_interval_ms,
            idle_base_ms,
            max_ms,
            current_idle_ms: idle_base_ms,
        }
    }

    /// Sensible defaults for DynamoDB Streams: 250 ms floor (~4/s), 1 s idle
    /// base, backing off to a 5 s cap.
    pub fn ddb_defaults() -> Self {
        Self::new(250, 1_000, 5_000)
    }

    /// Record the outcome of a poll and return how long to sleep before the next
    /// one. Records → reset to the catch-up floor (poll again promptly, but no
    /// faster than the throttle). Empty → back off exponentially toward the cap.
    pub fn next_sleep_ms(&mut self, got_records: bool) -> u64 {
        if got_records {
            self.current_idle_ms = self.idle_base_ms;
            self.min_interval_ms
        } else {
            let sleep = self.current_idle_ms;
            self.current_idle_ms = self.current_idle_ms.saturating_mul(2).min(self.max_ms);
            sleep
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catch_up_polls_at_the_throttle_floor() {
        let mut b = PollBackoff::new(250, 1_000, 5_000);
        // While records keep coming, we poll at the 250 ms floor (never faster).
        assert_eq!(b.next_sleep_ms(true), 250);
        assert_eq!(b.next_sleep_ms(true), 250);
    }

    #[test]
    fn idle_backs_off_exponentially_to_the_cap() {
        let mut b = PollBackoff::new(250, 1_000, 5_000);
        assert_eq!(b.next_sleep_ms(false), 1_000);
        assert_eq!(b.next_sleep_ms(false), 2_000);
        assert_eq!(b.next_sleep_ms(false), 4_000);
        assert_eq!(b.next_sleep_ms(false), 5_000, "capped at max");
        assert_eq!(b.next_sleep_ms(false), 5_000, "stays capped");
    }

    #[test]
    fn records_reset_the_idle_backoff() {
        let mut b = PollBackoff::new(250, 1_000, 5_000);
        b.next_sleep_ms(false); // 1000
        b.next_sleep_ms(false); // 2000
        // A record arrives → reset. Next idle starts from the base again.
        assert_eq!(b.next_sleep_ms(true), 250);
        assert_eq!(b.next_sleep_ms(false), 1_000, "idle base restored after data");
    }

    #[test]
    fn ddb_defaults_are_sane() {
        let mut b = PollBackoff::ddb_defaults();
        assert_eq!(b.next_sleep_ms(true), 250);
        assert_eq!(b.next_sleep_ms(false), 1_000);
    }
}
