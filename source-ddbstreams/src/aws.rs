//! Live async adapter over `aws-sdk-dynamodbstreams` (Apache-2.0 AWS SDK).
//!
//! This is the thin glue that turns real `DescribeStream` / `GetShardIterator` /
//! `GetRecords` calls into the values the pure engine consumes. All of the
//! correctness-critical shard-graph logic lives in the parent module and is
//! reused here: [`crate::build_shard_graph`], [`crate::close_open_parents`].
//!
//! Compiled only under the `aws` feature (needs the AWS SDK + a tokio runtime).
//! Grounded in `DynamoDBStreamsShardDetector` / `DynamoDBStreamsDataFetcher`
//! (awslabs/dynamodb-streams-kinesis-adapter, Apache-2.0). See core/REFERENCES.md.

use crate::{build_shard_graph, close_open_parents, DdbShard};
use amazon_dynamodb_streams_consumer_core::{Record, RecordBatch, ShardMeta, StartPosition};
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use aws_sdk_dynamodbstreams::Client;
use std::collections::HashMap;
use std::sync::Mutex;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A threaded shard iterator: the `next_shard_iterator` handed back by the last
/// `GetRecords`, plus the logical position (`after`) it continues from. Reusing
/// it avoids a `GetShardIterator` call per poll — this is KCL's
/// `DynamoDBStreamsDataFetcher` behavior (hold the iterator, re-derive only on
/// reposition/expiry).
#[derive(Clone)]
struct Cursor {
    /// The `after` checkpoint this iterator continues from (`None` = TRIM_HORIZON).
    after: Option<String>,
    iterator: String,
}

/// A live DynamoDB Streams source bound to one stream ARN.
pub struct DdbStreamsSource {
    client: Client,
    stream_arn: String,
    /// Per-shard threaded iterators (shard id -> next iterator + its position).
    cursors: Mutex<HashMap<String, Cursor>>,
    /// Optional `GetRecords` batch-size limit (`None` = service default). DynamoDB
    /// Streams accepts 1..=1000; see [`clamp_max_records`].
    max_records: Option<i32>,
}

/// DynamoDB Streams `GetRecords` accepts a `Limit` of 1..=1000; clamp any
/// caller-supplied value into that range so we never send a request the service
/// would reject.
fn clamp_max_records(n: i32) -> i32 {
    n.clamp(1, 1000)
}

impl DdbStreamsSource {
    pub fn new(client: Client, stream_arn: impl Into<String>) -> Self {
        Self {
            client,
            stream_arn: stream_arn.into(),
            cursors: Mutex::new(HashMap::new()),
            max_records: None,
        }
    }

    /// Set the `GetRecords` batch-size limit (clamped to DynamoDB Streams'
    /// 1..=1000). `None`/unset uses the service default.
    pub fn with_max_records(mut self, n: i32) -> Self {
        self.max_records = Some(clamp_max_records(n));
        self
    }

    /// Build a source from the ambient AWS environment (creds, region).
    pub async fn from_env(stream_arn: impl Into<String>) -> Self {
        let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Self::new(Client::new(&cfg), stream_arn)
    }

    /// Full paginated `DescribeStream` → normalized shards → `close_open_parents`
    /// → shard-graph lineage. This is the live `describe_shards`.
    pub async fn describe_shards(&self) -> Result<Vec<ShardMeta>, BoxError> {
        let mut raw: Vec<DdbShard> = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let resp = self
                .client
                .describe_stream()
                .stream_arn(&self.stream_arn)
                .set_exclusive_start_shard_id(start.clone())
                .send()
                .await?;
            let Some(desc) = resp.stream_description() else {
                break;
            };
            for s in desc.shards() {
                let shard_id = s.shard_id().unwrap_or_default().to_string();
                if shard_id.is_empty() {
                    continue;
                }
                let parent_shard_id = s.parent_shard_id().map(|p| p.to_string());
                let ending_sequence_number = s
                    .sequence_number_range()
                    .and_then(|r| r.ending_sequence_number())
                    .map(|e| e.to_string());
                raw.push(DdbShard {
                    shard_id,
                    parent_shard_id,
                    ending_sequence_number,
                });
            }
            match desc.last_evaluated_shard_id() {
                Some(id) => start = Some(id.to_string()),
                None => break,
            }
        }
        // Phase 2 (close open parents) then build the lineage graph.
        let normalized = close_open_parents(raw);
        Ok(build_shard_graph(vec![normalized]))
    }

    /// Efficient incremental discovery: fetch ONLY the children of `parent` via
    /// `DescribeStream` with a `CHILD_SHARDS` `ShardFilter` (paginated), instead
    /// of re-scanning the whole stream. This is what lets the shard-sync leader
    /// stay quiet on a stable topology and query only when a shard ends.
    ///
    /// The returned children keep their `ParentShardId` link verbatim (we do NOT
    /// run `build_shard_graph`/`close_open_parents` here, since those would drop
    /// `parent` — which is intentionally absent from this filtered response — and
    /// wrongly root the children). `parent` is known-present in the lease table.
    ///
    /// Grounded in KCA `DynamoDBStreamsShardDetector.listShardsWithFilter`
    /// (`ShardFilterType.CHILD_SHARDS`, awslabs/dynamodb-streams-kinesis-adapter,
    /// Apache-2.0). On a filtered-call error the caller falls back to a full
    /// `describe_shards`, matching the adapter.
    pub async fn describe_child_shards(&self, parent: &str) -> Result<Vec<ShardMeta>, BoxError> {
        let filter = aws_sdk_dynamodbstreams::types::ShardFilter::builder()
            .r#type(aws_sdk_dynamodbstreams::types::ShardFilterType::ChildShards)
            .shard_id(parent)
            .build();
        let mut out: Vec<ShardMeta> = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let resp = self
                .client
                .describe_stream()
                .stream_arn(&self.stream_arn)
                .shard_filter(filter.clone())
                .set_exclusive_start_shard_id(start.clone())
                .send()
                .await?;
            let Some(desc) = resp.stream_description() else {
                break;
            };
            for s in desc.shards() {
                let shard_id = s.shard_id().unwrap_or_default().to_string();
                if shard_id.is_empty() {
                    continue;
                }
                out.push(ShardMeta {
                    id: shard_id,
                    parents: s
                        .parent_shard_id()
                        .map(|p| p.to_string())
                        .into_iter()
                        .collect(),
                });
            }
            match desc.last_evaluated_shard_id() {
                Some(id) => start = Some(id.to_string()),
                None => break,
            }
        }
        Ok(out)
    }

    /// Derive a *fresh* iterator from the stream via `GetShardIterator`. The
    /// starting position is decoded from the opaque checkpoint `after`
    /// (`AFTER_SEQUENCE_NUMBER` when resuming from a real sequence number, else
    /// the seeded start mode — `TRIM_HORIZON` by default). Used on first read,
    /// reposition, or after an expired/trimmed iterator — not on the steady-state
    /// poll path.
    async fn derive_iterator(
        &self,
        shard: &str,
        after: Option<&str>,
    ) -> Result<Option<String>, BoxError> {
        let (iter_type, seq) = match StartPosition::from_checkpoint(after) {
            StartPosition::After(s) => (ShardIteratorType::AfterSequenceNumber, Some(s)),
            StartPosition::Latest => (ShardIteratorType::Latest, None),
            StartPosition::TrimHorizon => (ShardIteratorType::TrimHorizon, None),
            // Non-exhaustive: any future start mode falls back to TRIM_HORIZON
            // until it derives its own iterator type here.
            _ => (ShardIteratorType::TrimHorizon, None),
        };
        let resp = self
            .client
            .get_shard_iterator()
            .stream_arn(&self.stream_arn)
            .shard_id(shard)
            .shard_iterator_type(iter_type)
            .set_sequence_number(seq)
            .send()
            .await?;
        Ok(resp.shard_iterator().map(|s| s.to_string()))
    }

    /// Return a reusable threaded iterator for `shard` iff a cached cursor
    /// continues from exactly the requested `after` position. A mismatch means
    /// the caller is repositioning (or this is a fresh/restarted process), so we
    /// must not reuse.
    fn cached_iterator(&self, shard: &str, after: Option<&str>) -> Option<String> {
        let cursors = self.cursors.lock().unwrap();
        cursors
            .get(shard)
            .filter(|c| cursor_continues(c.after.as_deref(), after))
            .map(|c| c.iterator.clone())
    }

    fn store_cursor(&self, shard: &str, after: Option<String>, iterator: Option<String>) {
        let mut cursors = self.cursors.lock().unwrap();
        match iterator {
            Some(it) => {
                cursors.insert(
                    shard.to_string(),
                    Cursor {
                        after,
                        iterator: it,
                    },
                );
            }
            None => {
                cursors.remove(shard); // SHARD_END → nothing more to thread.
            }
        }
    }

    fn drop_cursor(&self, shard: &str) {
        self.cursors.lock().unwrap().remove(shard);
    }

    /// One `GetRecords` round after the opaque checkpoint `after` (`None` =
    /// `TRIM_HORIZON`). Returns the batch and whether the shard is closed
    /// (`next_shard_iterator == None` → SHARD_END).
    ///
    /// Reuses the threaded `next_shard_iterator` from the previous poll when it
    /// continues from the same `after` (avoiding a `GetShardIterator` per call);
    /// otherwise derives a fresh iterator. Self-heals the two recoverable
    /// iterator failures the adapter is expected to handle:
    /// `TrimmedDataAccessException` and `ExpiredIteratorException` → drop the
    /// stale cursor and restart from `after`/`TRIM_HORIZON` (matches
    /// `DynamoDBStreamsDataFetcher`).
    pub async fn get_records(
        &self,
        shard: &str,
        after: Option<&str>,
    ) -> Result<RecordBatch, BoxError> {
        // 1) Obtain an iterator: reuse the threaded one, else derive fresh.
        let iterator = match self.cached_iterator(shard, after) {
            Some(it) => it,
            None => match self.derive_iterator(shard, after).await {
                Ok(Some(it)) => it,
                Ok(None) => {
                    self.drop_cursor(shard);
                    return Ok(RecordBatch {
                        records: vec![],
                        shard_end: true,
                        millis_behind_latest: None,
                    });
                }
                Err(e) if is_recoverable(&e) && after.is_some() => {
                    // Checkpoint too old → restart at TRIM_HORIZON.
                    match self.derive_iterator(shard, None).await? {
                        Some(it) => it,
                        None => {
                            return Ok(RecordBatch {
                                records: vec![],
                                shard_end: true,
                                millis_behind_latest: None,
                            })
                        }
                    }
                }
                Err(e) => return Err(e),
            },
        };

        // 2) GetRecords, self-healing an expired/trimmed threaded iterator once,
        //    and retrying transient throttling with bounded backoff.
        let resp = match with_throttle_retry(|| async {
            self.client
                .get_records()
                .shard_iterator(&iterator)
                .set_limit(self.max_records)
                .send()
                .await
                .map_err(|e| -> BoxError { e.into() })
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                let be: BoxError = e;
                if is_recoverable(&be) {
                    // The (possibly cached) iterator expired → drop it and
                    // re-derive from the checkpoint, retrying once.
                    self.drop_cursor(shard);
                    let fresh = match self.derive_iterator(shard, after).await? {
                        Some(it) => it,
                        None => {
                            return Ok(RecordBatch {
                                records: vec![],
                                shard_end: true,
                                millis_behind_latest: None,
                            })
                        }
                    };
                    self.client
                        .get_records()
                        .shard_iterator(&fresh)
                        .set_limit(self.max_records)
                        .send()
                        .await?
                } else {
                    return Err(be);
                }
            }
        };

        let mut records = Vec::new();
        let mut newest_creation_ms: Option<i64> = None;
        for r in resp.records() {
            if let Some(sr) = r.dynamodb() {
                let seq = sr.sequence_number().unwrap_or_default().to_string();
                if seq.is_empty() {
                    continue;
                }
                // Track the newest record's creation time for MillisBehindLatest
                // (records arrive in ascending order → last wins).
                if let Some(t) = sr.approximate_creation_date_time() {
                    newest_creation_ms = Some(t.to_millis().unwrap_or_else(|_| t.secs() * 1000));
                }
                // Carry the full typed change record (Keys/NewImage/OldImage/
                // eventName) as the opaque payload, per KCL's RecordAdapter model.
                let payload = crate::record::from_sdk(r).encode();
                records.push(Record {
                    shard_id: shard.to_string(),
                    seq,
                    data: payload,
                });
            }
        }
        // 3) Thread the next iterator. The cursor's logical position advances to
        // the last delivered seq (or stays at `after` if this poll was empty).
        let next = resp.next_shard_iterator().map(|s| s.to_string());
        let new_after = advanced_after(after, records.last().map(|r| r.seq.as_str()));
        self.store_cursor(shard, new_after, next.clone());
        // A closed shard yields no next iterator.
        let shard_end = next.is_none();
        // Consumer lag: now - newest record's ApproximateCreationDateTime. DDB
        // Streams GetRecords has no MillisBehindLatest field, so we derive it
        // (matching KCA). Clamped at 0 to absorb minor clock skew.
        let millis_behind_latest = newest_creation_ms.map(|created| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(created);
            (now - created).max(0)
        });
        Ok(RecordBatch {
            records,
            shard_end,
            millis_behind_latest,
        })
    }
}

/// A cached cursor continues the caller's read iff it is positioned at exactly
/// the requested `after`. A mismatch means a reposition (or fresh/restarted
/// process), so the threaded iterator must NOT be reused.
fn cursor_continues(cursor_after: Option<&str>, requested: Option<&str>) -> bool {
    cursor_after == requested
}

/// The cursor's new logical position after a poll: the last delivered seq, or
/// the unchanged `requested` position if the poll was empty.
fn advanced_after(requested: Option<&str>, last_seq: Option<&str>) -> Option<String> {
    last_seq.or(requested).map(|s| s.to_string())
}

/// Trimmed-data / expired-iterator / resource-not-found are recoverable by
/// restarting the shard at `TRIM_HORIZON`.
fn is_recoverable(e: &BoxError) -> bool {
    let msg = e.to_string();
    msg.contains("TrimmedDataAccessException")
        || msg.contains("ExpiredIteratorException")
        || msg.contains("ResourceNotFoundException")
}

/// DynamoDB Streams throttling / capacity errors — retryable with backoff
/// (DDB Streams meters `GetRecords` to ~4 calls/s/shard).
fn is_throttling(e: &BoxError) -> bool {
    let msg = e.to_string();
    msg.contains("ProvisionedThroughputExceededException")
        || msg.contains("ThrottlingException")
        || msg.contains("RequestLimitExceeded")
        || msg.contains("LimitExceededException")
}

/// Run a `GetRecords` send, retrying only on throttling with capped exponential
/// backoff (~100ms→800ms, 5 attempts). Non-throttle errors (including the
/// recoverable expired/trimmed cases) return immediately for the caller's
/// self-heal path.
async fn with_throttle_retry<T, F, Fut>(mut op: F) -> Result<T, BoxError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, BoxError>>,
{
    let mut delay_ms = 100u64;
    for attempt in 0..5 {
        match op().await {
            Err(e) if is_throttling(&e) && attempt < 4 => {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(800);
            }
            other => return other,
        }
    }
    unreachable!("loop returns on the final attempt")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_records_is_clamped_to_streams_valid_range() {
        // DynamoDB Streams GetRecords accepts a Limit of 1..=1000.
        assert_eq!(clamp_max_records(500), 500); // in range: unchanged
        assert_eq!(clamp_max_records(1), 1); // lower bound
        assert_eq!(clamp_max_records(1000), 1000); // upper bound
        assert_eq!(clamp_max_records(0), 1); // below range -> 1
        assert_eq!(clamp_max_records(-7), 1); // negative -> 1
        assert_eq!(clamp_max_records(5000), 1000); // above range -> 1000
    }

    #[test]
    fn cursor_reused_only_when_it_continues_from_requested_position() {
        // Same position → reuse the threaded iterator.
        assert!(cursor_continues(Some("seq-5"), Some("seq-5")));
        assert!(cursor_continues(None, None)); // both at TRIM_HORIZON
                                               // Reposition / restart → do not reuse.
        assert!(!cursor_continues(Some("seq-5"), Some("seq-9")));
        assert!(!cursor_continues(Some("seq-5"), None));
        assert!(!cursor_continues(None, Some("seq-5")));
    }

    #[test]
    fn cursor_position_advances_to_last_seq_else_holds() {
        // Records delivered → advance to the last seq.
        assert_eq!(advanced_after(Some("5"), Some("8")).as_deref(), Some("8"));
        assert_eq!(advanced_after(None, Some("1")).as_deref(), Some("1"));
        // Empty poll → hold the requested position (open shard keeps polling).
        assert_eq!(advanced_after(Some("5"), None).as_deref(), Some("5"));
        assert_eq!(advanced_after(None, None), None);
    }

    #[test]
    fn recoverable_errors_are_classified() {
        let mk = |s: &str| -> BoxError { s.to_string().into() };
        assert!(is_recoverable(&mk(
            "ExpiredIteratorException: iterator expired"
        )));
        assert!(is_recoverable(&mk(
            "com.amazonaws...TrimmedDataAccessException"
        )));
        assert!(is_recoverable(&mk("ResourceNotFoundException")));
        assert!(!is_recoverable(&mk("ValidationException: bad input")));
        assert!(!is_recoverable(&mk("some other service error")));
    }

    #[test]
    fn throttling_errors_are_classified() {
        let mk = |s: &str| -> BoxError { s.to_string().into() };
        assert!(is_throttling(&mk("ProvisionedThroughputExceededException")));
        assert!(is_throttling(&mk("ThrottlingException: rate exceeded")));
        assert!(is_throttling(&mk("RequestLimitExceeded")));
        assert!(is_throttling(&mk("LimitExceededException")));
        // Recoverable-but-not-throttle and hard errors are not retried as throttles.
        assert!(!is_throttling(&mk("ExpiredIteratorException")));
        assert!(!is_throttling(&mk("ValidationException: bad input")));
    }

    #[test]
    fn throttle_retry_retries_throttles_then_passes_through_others() {
        use std::cell::Cell;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        // Throttled twice, then succeeds → 3 total attempts.
        let calls = Cell::new(0u32);
        let ok: Result<u8, BoxError> = rt.block_on(with_throttle_retry(|| {
            let n = calls.get();
            calls.set(n + 1);
            async move {
                if n < 2 {
                    Err("ThrottlingException".to_string().into())
                } else {
                    Ok(7u8)
                }
            }
        }));
        assert_eq!(ok.unwrap(), 7);
        assert_eq!(
            calls.get(),
            3,
            "should retry the two throttles then succeed"
        );

        // A non-throttle error returns immediately (no retry).
        let calls2 = Cell::new(0u32);
        let err: Result<u8, BoxError> = rt.block_on(with_throttle_retry(|| {
            calls2.set(calls2.get() + 1);
            async move { Err::<u8, BoxError>("ExpiredIteratorException".to_string().into()) }
        }));
        assert!(err.is_err());
        assert_eq!(calls2.get(), 1, "non-throttle errors must not be retried");
    }
}
