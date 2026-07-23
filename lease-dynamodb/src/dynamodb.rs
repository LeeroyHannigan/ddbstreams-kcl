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
    PointInTimeRecoverySpecification, ProvisionedThroughput, ReturnValue, ScalarAttributeType,
    TableStatus,
};
use aws_sdk_dynamodb::Client;
use std::collections::HashMap;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const LEASE_KEY: &str = "leaseKey";
const LEASE_OWNER: &str = "leaseOwner";
const LEASE_COUNTER: &str = "leaseCounter";
const CHECKPOINT: &str = "checkpoint";
const COMPLETED: &str = "completed";
const PARENTS: &str = "parentShardIds";

/// Result of a conditional lease mutation.
#[derive(Debug)]
pub enum LeaseError {
    /// The optimistic-lock condition failed — another worker owns/advanced it.
    Lost,
    /// A throttling/capacity error (retryable with backoff).
    Throttled(BoxError),
    Aws(BoxError),
}
impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Lost => write!(f, "lease lost (conditional check failed)"),
            LeaseError::Throttled(e) => write!(f, "throttled: {e}"),
            LeaseError::Aws(e) => write!(f, "aws error: {e}"),
        }
    }
}
impl std::error::Error for LeaseError {}

const CONDITIONAL_CHECK_FAILED: &str = "ConditionalCheckFailedException";
// DynamoDB throttling / capacity error codes — retryable with backoff.
const THROTTLE_CODES: &[&str] = &[
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
    "LimitExceededException",
];

pub struct DynamoDbLeaseStore {
    client: Client,
    table: String,
    table_config: LeaseTableConfig,
}

/// Billing mode applied when [`DynamoDbLeaseStore::ensure_table`] auto-creates
/// the lease table.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum LeaseBilling {
    /// On-demand capacity (default) — nothing to provision.
    #[default]
    PayPerRequest,
    /// Provisioned capacity with fixed read/write units.
    Provisioned {
        read_capacity: i64,
        write_capacity: i64,
    },
}

/// Options applied ONLY when the lease table is auto-created (never mutated on a
/// pre-existing, user-managed table). Mirrors KCL's lease-table billing and
/// point-in-time-recovery knobs.
#[derive(Clone, Debug, Default)]
pub struct LeaseTableConfig {
    pub billing: LeaseBilling,
    /// Enable point-in-time recovery (PITR) on the freshly-created table.
    /// Requires the `dynamodb:UpdateContinuousBackups` permission.
    pub pitr: bool,
}

impl DynamoDbLeaseStore {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
            table_config: LeaseTableConfig::default(),
        }
    }

    /// Set the options used when `ensure_table` auto-creates the lease table
    /// (billing mode, PITR). Ignored if the table already exists.
    pub fn with_table_config(mut self, table_config: LeaseTableConfig) -> Self {
        self.table_config = table_config;
        self
    }

    pub async fn from_env(table: impl Into<String>) -> Self {
        let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Self::new(Client::new(&cfg), table)
    }

    /// Create the lease table (PK `leaseKey`) if absent, applying the configured
    /// billing mode (default PAY_PER_REQUEST) and, on a freshly-created table,
    /// enabling PITR when requested; then wait until ACTIVE. Idempotent.
    pub async fn ensure_table(&self) -> Result<(), BoxError> {
        let exists = self
            .client
            .describe_table()
            .table_name(&self.table)
            .send()
            .await
            .is_ok();
        if !exists {
            let mut create = self
                .client
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
                );
            create = match &self.table_config.billing {
                LeaseBilling::PayPerRequest => create.billing_mode(BillingMode::PayPerRequest),
                LeaseBilling::Provisioned {
                    read_capacity,
                    write_capacity,
                } => create
                    .billing_mode(BillingMode::Provisioned)
                    .provisioned_throughput(
                        ProvisionedThroughput::builder()
                            .read_capacity_units(*read_capacity)
                            .write_capacity_units(*write_capacity)
                            .build()?,
                    ),
            };
            create.send().await?;
        }
        loop {
            let d = self
                .client
                .describe_table()
                .table_name(&self.table)
                .send()
                .await?;
            if d.table().and_then(|t| t.table_status()) == Some(&TableStatus::Active) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        // PITR is only applied when WE created the table (never mutate a
        // pre-existing, user-managed table) and only if requested. Requires
        // dynamodb:UpdateContinuousBackups. A freshly-created table's backup
        // subsystem may not be ready for a moment, in which case
        // UpdateContinuousBackups returns a retryable
        // ContinuousBackupsUnavailableException; retry with linear backoff.
        if !exists && self.table_config.pitr {
            let mut attempt: u32 = 0;
            loop {
                let r = self
                    .client
                    .update_continuous_backups()
                    .table_name(&self.table)
                    .point_in_time_recovery_specification(
                        PointInTimeRecoverySpecification::builder()
                            .point_in_time_recovery_enabled(true)
                            .build()?,
                    )
                    .send()
                    .await;
                match r {
                    Ok(_) => break,
                    Err(e)
                        if attempt < 12
                            && e.code() == Some("ContinuousBackupsUnavailableException") =>
                    {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                            .await;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
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
                    parents: vec![],
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

    async fn claim(
        &self,
        lease_key: &str,
        owner: &str,
        seen_counter: u64,
    ) -> Result<Lease, LeaseError> {
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
                parents: vec![],
            }),
            Err(e) => Err(classify(e)),
        }
    }

    /// Heartbeat: bump the counter, conditioned on still owning it at the counter
    /// we hold. Returns the new counter, or `LeaseError::Lost` if stolen.
    pub async fn renew(
        &self,
        lease_key: &str,
        owner: &str,
        counter: u64,
    ) -> Result<u64, LeaseError> {
        with_throttle_retry(|| async {
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
        })
        .await
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
        with_throttle_retry(|| async {
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
        })
        .await
    }

    /// Bump this worker's heartbeat row (`__hb__:<worker>` — matches
    /// `core::coordinator::HEARTBEAT_KEY_PREFIX`), creating it at counter 1 if
    /// absent. Unconditional: a worker owns its own heartbeat key, so there is
    /// no optimistic-lock contention. This is the single per-worker liveness
    /// write that replaces per-shard renews. Returns the new counter.
    pub async fn heartbeat(&self, worker: &str) -> Result<u64, LeaseError> {
        let key = format!("__hb__:{worker}");
        with_throttle_retry(|| async {
            let r = self
                .client
                .update_item()
                .table_name(&self.table)
                .key(LEASE_KEY, AttributeValue::S(key.clone()))
                // ADD creates leaseCounter at :one if the row/attr is absent.
                .update_expression("SET leaseOwner = :w ADD leaseCounter :one")
                .expression_attribute_values(":w", AttributeValue::S(worker.to_string()))
                .expression_attribute_values(":one", AttributeValue::N("1".into()))
                .return_values(ReturnValue::UpdatedNew)
                .send()
                .await;
            match r {
                Ok(o) => Ok(o
                    .attributes()
                    .and_then(|a| a.get(LEASE_COUNTER))
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0)),
                Err(e) => Err(classify(e)),
            }
        })
        .await
    }

    /// Mark the shard fully processed (SHARD_END), conditioned on ownership.
    pub async fn mark_complete(
        &self,
        lease_key: &str,
        owner: &str,
        counter: u64,
    ) -> Result<(), LeaseError> {
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

    /// Delete a completed shard's lease (SHARD_END tombstone GC). Conditioned on
    /// `completed = true` so a still-active or resurrected lease is never
    /// removed. A `LeaseError::Lost` means the condition failed (not completed /
    /// already gone) — harmless; the caller skips it. Mirrors KCL
    /// `LeaseCleanupManager.cleanupLeaseForCompletedShard`.
    pub async fn delete_lease(&self, lease_key: &str) -> Result<(), LeaseError> {
        let r = self
            .client
            .delete_item()
            .table_name(&self.table)
            .key(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .condition_expression("attribute_exists(leaseKey) AND completed = :t")
            .expression_attribute_values(":t", AttributeValue::Bool(true))
            .send()
            .await;
        match r {
            Ok(_) => Ok(()),
            Err(e) => Err(classify(e)),
        }
    }

    /// Release a lease we hold: clear the owner and bump the counter, conditioned
    /// on ownership. Lets another worker take it over **immediately** on graceful
    /// shutdown instead of waiting for the lease to expire (KCL evicts on
    /// shutdown). A `LeaseError::Lost` means it was already stolen — harmless.
    pub async fn release(
        &self,
        lease_key: &str,
        owner: &str,
        counter: u64,
    ) -> Result<(), LeaseError> {
        let r = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .update_expression("REMOVE leaseOwner SET leaseCounter = leaseCounter + :one")
            .condition_expression("leaseCounter = :c AND leaseOwner = :o")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
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

impl DynamoDbLeaseStore {
    /// Publish a shard as an unowned lease carrying its parents, create-if-absent.
    /// Called only by the shard-sync leader. An existing lease (owned or not) is
    /// left untouched — the `attribute_not_exists` guard makes this idempotent, so
    /// a re-sync never clobbers in-progress ownership/checkpoint state.
    pub async fn create_shard_lease(
        &self,
        lease_key: &str,
        parents: &[String],
        checkpoint: Option<&str>,
    ) -> Result<(), LeaseError> {
        let mut put = self
            .client
            .put_item()
            .table_name(&self.table)
            .item(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
            .item(LEASE_COUNTER, AttributeValue::N("0".into()))
            .condition_expression("attribute_not_exists(leaseKey)");
        if let Some(cp) = checkpoint {
            put = put.item(CHECKPOINT, AttributeValue::S(cp.to_string()));
        }
        if !parents.is_empty() {
            put = put.item(
                PARENTS,
                AttributeValue::L(
                    parents
                        .iter()
                        .map(|p| AttributeValue::S(p.clone()))
                        .collect(),
                ),
            );
        }
        match put.send().await {
            Ok(_) => Ok(()),
            // Lease already exists → nothing to do (idempotent).
            Err(e) if e.code() == Some(CONDITIONAL_CHECK_FAILED) => Ok(()),
            Err(e) => Err(LeaseError::Aws(Box::new(e))),
        }
    }

    /// Optimistic leader-lease bid — see the `AsyncLeaseStore` trait doc.
    /// `None` = create-if-absent (vacant); `Some(c)` = steal an expired leader
    /// conditioned on counter `c`. Returns the held counter, or `None` if the
    /// race was lost.
    pub async fn try_acquire_leadership(
        &self,
        lease_key: &str,
        owner: &str,
        expected: Option<u64>,
    ) -> Result<Option<u64>, LeaseError> {
        match expected {
            None => {
                let r = self
                    .client
                    .put_item()
                    .table_name(&self.table)
                    .item(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
                    .item(LEASE_OWNER, AttributeValue::S(owner.to_string()))
                    .item(LEASE_COUNTER, AttributeValue::N("1".into()))
                    .condition_expression("attribute_not_exists(leaseKey)")
                    .send()
                    .await;
                match r {
                    Ok(_) => Ok(Some(1)),
                    Err(e) if e.code() == Some(CONDITIONAL_CHECK_FAILED) => Ok(None),
                    Err(e) => Err(LeaseError::Aws(Box::new(e))),
                }
            }
            Some(c) => {
                // Steal regardless of current owner, but only if the counter has
                // NOT advanced since we observed the expired lease.
                let r = self
                    .client
                    .update_item()
                    .table_name(&self.table)
                    .key(LEASE_KEY, AttributeValue::S(lease_key.to_string()))
                    .update_expression("SET leaseOwner = :o, leaseCounter = leaseCounter + :one")
                    .condition_expression("leaseCounter = :c")
                    .expression_attribute_values(":o", AttributeValue::S(owner.to_string()))
                    .expression_attribute_values(":one", AttributeValue::N("1".into()))
                    .expression_attribute_values(":c", AttributeValue::N(c.to_string()))
                    .send()
                    .await;
                match r {
                    Ok(_) => Ok(Some(c + 1)),
                    Err(e) if e.code() == Some(CONDITIONAL_CHECK_FAILED) => Ok(None),
                    Err(e) => Err(LeaseError::Aws(Box::new(e))),
                }
            }
        }
    }
}

fn classify<E>(e: E) -> LeaseError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    if e.code() == Some(CONDITIONAL_CHECK_FAILED) {
        LeaseError::Lost
    } else if e
        .code()
        .map(|c| THROTTLE_CODES.contains(&c))
        .unwrap_or(false)
    {
        LeaseError::Throttled(Box::new(e))
    } else {
        LeaseError::Aws(Box::new(e))
    }
}

/// Run a fallible lease op, retrying only on `Throttled` with capped exponential
/// backoff (~50ms→800ms, 5 attempts). `Lost` and other `Aws` errors return
/// immediately — a throttle must not be mistaken for lease loss (which would
/// drop the shard and cause needless failover churn).
async fn with_throttle_retry<T, F, Fut>(mut op: F) -> Result<T, LeaseError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, LeaseError>>,
{
    let mut delay_ms = 50u64;
    for attempt in 0..5 {
        match op().await {
            Err(LeaseError::Throttled(_)) if attempt < 4 => {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(800);
            }
            other => return other,
        }
    }
    unreachable!("loop returns on the final attempt")
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
        completed: item
            .get(COMPLETED)
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        parents: item
            .get(PARENTS)
            .and_then(|v| v.as_l().ok())
            .map(|l| {
                l.iter()
                    .filter_map(|v| v.as_s().ok().cloned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    #[test]
    fn classify_maps_conditional_throttle_and_other() {
        use aws_sdk_dynamodb::error::{ErrorMetadata, ProvideErrorMetadata};

        #[derive(Debug)]
        struct FakeErr(ErrorMetadata);
        impl FakeErr {
            fn with_code(code: &str) -> Self {
                FakeErr(ErrorMetadata::builder().code(code).build())
            }
        }
        impl std::fmt::Display for FakeErr {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.0.code())
            }
        }
        impl std::error::Error for FakeErr {}
        impl ProvideErrorMetadata for FakeErr {
            fn meta(&self) -> &ErrorMetadata {
                &self.0
            }
        }

        // Conditional-check failure => the optimistic lock was lost.
        assert!(matches!(
            classify(FakeErr::with_code(CONDITIONAL_CHECK_FAILED)),
            LeaseError::Lost
        ));
        // Each throttle code => retryable Throttled (must NOT be seen as Lost).
        for c in THROTTLE_CODES {
            assert!(
                matches!(classify(FakeErr::with_code(c)), LeaseError::Throttled(_)),
                "code {c} should classify as Throttled"
            );
        }
        // Any other error => opaque Aws (neither Lost nor Throttled).
        assert!(matches!(
            classify(FakeErr::with_code("ValidationException")),
            LeaseError::Aws(_)
        ));
    }

    #[test]
    fn throttle_retry_succeeds_after_transient_throttles() {
        let calls = Cell::new(0);
        let out: Result<u32, LeaseError> = rt().block_on(with_throttle_retry(|| {
            let n = calls.get() + 1;
            calls.set(n);
            async move {
                if n < 3 {
                    Err(LeaseError::Throttled("throttled".into()))
                } else {
                    Ok(42)
                }
            }
        }));
        assert_eq!(out.unwrap(), 42);
        assert_eq!(calls.get(), 3, "retried through the transient throttles");
    }

    #[test]
    fn throttle_retry_does_not_retry_lost() {
        let calls = Cell::new(0);
        let out: Result<u32, LeaseError> = rt().block_on(with_throttle_retry(|| {
            calls.set(calls.get() + 1);
            async { Err::<u32, _>(LeaseError::Lost) }
        }));
        assert!(matches!(out, Err(LeaseError::Lost)));
        assert_eq!(
            calls.get(),
            1,
            "a lost lease must never be retried as throttling"
        );
    }

    #[test]
    fn throttle_retry_gives_up_after_max_attempts() {
        let calls = Cell::new(0);
        let out: Result<u32, LeaseError> = rt().block_on(with_throttle_retry(|| {
            calls.set(calls.get() + 1);
            async { Err::<u32, _>(LeaseError::Throttled("throttled".into())) }
        }));
        assert!(matches!(out, Err(LeaseError::Throttled(_))));
        assert_eq!(calls.get(), 5, "bounded at 5 attempts");
    }
}
