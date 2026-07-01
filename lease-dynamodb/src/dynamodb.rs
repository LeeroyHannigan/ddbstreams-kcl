//! Async DynamoDB lease store (behind the `aws` feature).
//!
//! Optimistic locking on `leaseCounter`: acquire/renew/checkpoint/complete are
//! all conditional on the counter (and owner) the caller last read, so exactly
//! one worker wins each transition. Mirrors KCL `DynamoDBLeaseRefresher`
//! (Apache-2.0). See core/REFERENCES.md.

use crate::Lease;
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, TableStatus,
};
use aws_sdk_dynamodb::Client;
use std::collections::HashMap;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const LEASE_KEY: &str = "leaseKey";
const LEASE_OWNER: &str = "leaseOwner";
const LEASE_COUNTER: &str = "leaseCounter";
const CHECKPOINT: &str = "checkpoint";
const COMPLETED: &str = "completed";

/// Result of a conditional lease mutation.
#[derive(Debug)]
pub enum LeaseError {
    /// The optimistic-lock condition failed — another worker owns/advanced it.
    Lost,
    Aws(BoxError),
}
impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Lost => write!(f, "lease lost (conditional check failed)"),
            LeaseError::Aws(e) => write!(f, "aws error: {e}"),
        }
    }
}
impl std::error::Error for LeaseError {}

const CONDITIONAL_CHECK_FAILED: &str = "ConditionalCheckFailedException";

pub struct DynamoDbLeaseStore {
    client: Client,
    table: String,
}

impl DynamoDbLeaseStore {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self { client, table: table.into() }
    }

    pub async fn from_env(table: impl Into<String>) -> Self {
        let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Self::new(Client::new(&cfg), table)
    }

    /// Create the lease table (PK `leaseKey`, PAY_PER_REQUEST) if absent, and
    /// wait until ACTIVE. Idempotent.
    pub async fn ensure_table(&self) -> Result<(), BoxError> {
        let exists = self.client.describe_table().table_name(&self.table).send().await.is_ok();
        if !exists {
            self.client
                .create_table()
                .table_name(&self.table)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(LEASE_KEY)
                        .attribute_type(ScalarAttributeType::S)
                        .build()?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(LEASE_KEY)
                        .key_type(KeyType::Hash)
                        .build()?,
                )
                .billing_mode(BillingMode::PayPerRequest)
                .send()
                .await?;
        }
        loop {
            let d = self.client.describe_table().table_name(&self.table).send().await?;
            if d.table().and_then(|t| t.table_status()) == Some(&TableStatus::Active) {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    /// Scan every lease row in the table (used by the coordinator).
    pub async fn list_all(&self) -> Result<Vec<Lease>, BoxError> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let resp = self
                .client
                .scan()
                .table_name(&self.table)
                .set_exclusive_start_key(start)
                .send()
                .await?;
            for item in resp.items() {
                out.push(item_to_lease(item));
            }
            start = resp.last_evaluated_key().cloned();
            if start.is_none() {
                break;
            }
        }
        Ok(out)
    }

    pub async fn get(&self, lease_key: &str) -> Result<Option<Lease>, BoxError> {
        let resp = self
            .client
            .get_item()
            .table_name(&self.table)
            .key(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .consistent_read(true)
            .send()
            .await?;
        Ok(resp.item().map(item_to_lease))
    }

    /// Acquire a lease: create it (counter=1) if it doesn't exist, otherwise
    /// claim it by bumping the counter conditioned on the value we just read.
    pub async fn acquire(&self, lease_key: &str, owner: &str) -> Result<Lease, LeaseError> {
        // Try create-if-not-exists.
        let create = self
            .client
            .put_item()
            .table_name(&self.table)
            .item(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .item(LEASE_OWNER, AttributeValue::S(owner.to_string()))
            .item(LEASE_COUNTER, AttributeValue::N("1".into()))
            .condition_expression("attribute_not_exists(leaseKey)")
            .send()
            .await;
        match create {
            Ok(_) => {
                return Ok(Lease {
                    lease_key: lease_key.to_string(),
                    lease_owner: Some(owner.to_string()),
                    lease_counter: 1,
                    checkpoint: None,
                    completed: false,
                })
            }
            Err(e) => {
                if e.code() != Some(CONDITIONAL_CHECK_FAILED) {
                    return Err(LeaseError::Aws(Box::new(e)));
                }
                // Conditional check failed → the lease already exists; fall
                // through to claim it.
            }
        }
        // Exists → read current counter, then claim conditioned on it.
        let current = self.get(lease_key).await.map_err(LeaseError::Aws)?;
        let current = current.ok_or_else(|| LeaseError::Aws("lease vanished".into()))?;
        self.claim(lease_key, owner, current.lease_counter).await
    }

    async fn claim(&self, lease_key: &str, owner: &str, seen_counter: u64) -> Result<Lease, LeaseError> {
        let r = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .update_expression("SET leaseOwner = :o, leaseCounter = leaseCounter + :one")
            .condition_expression("leaseCounter = :c")
            .expression_attribute_values(":o", AttributeValue::S(owner.to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":c", AttributeValue::N(seen_counter.to_string()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(Lease {
                lease_key: lease_key.to_string(),
                lease_owner: Some(owner.to_string()),
                lease_counter: seen_counter + 1,
                checkpoint: None,
                completed: false,
            }),
            Err(e) => Err(classify(e)),
        }
    }

    /// Heartbeat: bump the counter, conditioned on still owning it at the counter
    /// we hold. Returns the new counter, or `LeaseError::Lost` if stolen.
    pub async fn renew(&self, lease_key: &str, owner: &str, counter: u64) -> Result<u64, LeaseError> {
        let r = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .update_expression("SET leaseCounter = leaseCounter + :one")
            .condition_expression("leaseCounter = :c AND leaseOwner = :o")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":c", AttributeValue::N(counter.to_string()))
            .expression_attribute_values(":o", AttributeValue::S(owner.to_string()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(counter + 1),
            Err(e) => Err(classify(e)),
        }
    }

    /// Persist an opaque checkpoint and bump the counter, conditioned on
    /// ownership. Returns the new counter.
    pub async fn checkpoint(
        &self,
        lease_key: &str,
        owner: &str,
        counter: u64,
        seq: &str,
    ) -> Result<u64, LeaseError> {
        let r = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .update_expression("SET checkpoint = :cp, leaseCounter = leaseCounter + :one")
            .condition_expression("leaseCounter = :c AND leaseOwner = :o")
            .expression_attribute_values(":cp", AttributeValue::S(seq.to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":c", AttributeValue::N(counter.to_string()))
            .expression_attribute_values(":o", AttributeValue::S(owner.to_string()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(counter + 1),
            Err(e) => Err(classify(e)),
        }
    }

    /// Mark the shard fully processed (SHARD_END), conditioned on ownership.
    pub async fn mark_complete(&self, lease_key: &str, owner: &str, counter: u64) -> Result<(), LeaseError> {
        let r = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .update_expression("SET completed = :t")
            .condition_expression("leaseCounter = :c AND leaseOwner = :o")
            .expression_attribute_values(":t", AttributeValue::Bool(true))
            .expression_attribute_values(":c", AttributeValue::N(counter.to_string()))
            .expression_attribute_values(":o", AttributeValue::S(owner.to_string()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(()),
            Err(e) => Err(classify(e)),
        }
    }
}

fn classify<E>(e: E) -> LeaseError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    if e.code() == Some(CONDITIONAL_CHECK_FAILED) {
        LeaseError::Lost
    } else {
        LeaseError::Aws(Box::new(e))
    }
}

fn item_to_lease(item: &HashMap<String, AttributeValue>) -> Lease {
    let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    let n = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok())
    };
    Lease {
        lease_key: s(LEASE_KEY).unwrap_or_default(),
        lease_owner: s(LEASE_OWNER),
        lease_counter: n(LEASE_COUNTER).unwrap_or(0),
        checkpoint: s(CHECKPOINT),
        completed: item.get(COMPLETED).and_then(|v| v.as_bool().ok()).copied().unwrap_or(false),
    }
}
