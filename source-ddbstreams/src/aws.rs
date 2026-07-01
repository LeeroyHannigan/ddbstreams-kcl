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
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use aws_sdk_dynamodbstreams::Client;
use ddbstreams_kcl_core::{Record, RecordBatch, ShardMeta};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A live DynamoDB Streams source bound to one stream ARN.
pub struct DdbStreamsSource {
    client: Client,
    stream_arn: String,
}

impl DdbStreamsSource {
    pub fn new(client: Client, stream_arn: impl Into<String>) -> Self {
        Self {
            client,
            stream_arn: stream_arn.into(),
        }
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

    async fn shard_iterator(
        &self,
        shard: &str,
        after: Option<&str>,
    ) -> Result<Option<String>, BoxError> {
        let (iter_type, seq) = match after {
            Some(s) => (ShardIteratorType::AfterSequenceNumber, Some(s.to_string())),
            None => (ShardIteratorType::TrimHorizon, None),
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

    /// One `GetRecords` round after the opaque checkpoint `after` (`None` =
    /// `TRIM_HORIZON`). Returns the batch and whether the shard is closed
    /// (`next_shard_iterator == None` → SHARD_END).
    ///
    /// Self-heals the two recoverable iterator failures the adapter is expected
    /// to handle: `TrimmedDataAccessException` and `ExpiredIteratorException` →
    /// restart the shard at `TRIM_HORIZON` (matches `DynamoDBStreamsDataFetcher`).
    pub async fn get_records(
        &self,
        shard: &str,
        after: Option<&str>,
    ) -> Result<RecordBatch, BoxError> {
        let iterator = match self.shard_iterator(shard, after).await {
            Ok(Some(it)) => it,
            Ok(None) => return Ok(RecordBatch { records: vec![], shard_end: true }),
            Err(e) if is_recoverable(&e) && after.is_some() => {
                // Checkpoint too old / iterator expired → restart at TRIM_HORIZON.
                match self.shard_iterator(shard, None).await? {
                    Some(it) => it,
                    None => return Ok(RecordBatch { records: vec![], shard_end: true }),
                }
            }
            Err(e) => return Err(e),
        };

        let resp = match self.client.get_records().shard_iterator(&iterator).send().await {
            Ok(r) => r,
            Err(e) => {
                let be: BoxError = e.into();
                if is_recoverable(&be) {
                    // Expired between GetShardIterator and GetRecords → let the
                    // engine retry on the next describe cycle.
                    return Ok(RecordBatch { records: vec![], shard_end: false });
                }
                return Err(be);
            }
        };

        let mut records = Vec::new();
        for r in resp.records() {
            if let Some(sr) = r.dynamodb() {
                let seq = sr.sequence_number().unwrap_or_default().to_string();
                if seq.is_empty() {
                    continue;
                }
                // Payload mapping (Keys/NewImage/OldImage → bytes) is a follow-up;
                // the engine only needs the ordered sequence + shard for now.
                records.push(Record {
                    shard_id: shard.to_string(),
                    seq,
                    data: Vec::new(),
                });
            }
        }
        // A closed shard yields no next iterator.
        let shard_end = resp.next_shard_iterator().is_none();
        Ok(RecordBatch { records, shard_end })
    }
}

/// Trimmed-data / expired-iterator / resource-not-found are recoverable by
/// restarting the shard at `TRIM_HORIZON`.
fn is_recoverable(e: &BoxError) -> bool {
    let msg = e.to_string();
    msg.contains("TrimmedDataAccessException")
        || msg.contains("ExpiredIteratorException")
        || msg.contains("ResourceNotFoundException")
}
