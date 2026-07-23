//! Concrete AWS wiring: implement the worker's async traits for the live
//! `DdbStreamsSource` and `DynamoDbLeaseStore`. Behind the `aws` feature.

use crate::{AsyncLeaseStore, AsyncStreamSource, LeaseHandle, LeaseView, WorkerError};
use amazon_dynamodb_streams_consumer_core::{RecordBatch, ShardMeta};
use amazon_dynamodb_streams_consumer_lease::dynamodb::{DynamoDbLeaseStore, LeaseError};
use amazon_dynamodb_streams_consumer_source::aws::DdbStreamsSource;

#[async_trait::async_trait]
impl AsyncStreamSource for DdbStreamsSource {
    async fn describe_shards(&self) -> Result<Vec<ShardMeta>, WorkerError> {
        DdbStreamsSource::describe_shards(self).await
    }
    async fn get_records(
        &self,
        shard: &str,
        after: Option<String>,
    ) -> Result<RecordBatch, WorkerError> {
        DdbStreamsSource::get_records(self, shard, after.as_deref()).await
    }
    async fn describe_child_shards(&self, parent: &str) -> Result<Vec<ShardMeta>, WorkerError> {
        DdbStreamsSource::describe_child_shards(self, parent).await
    }
}

fn box_lease(e: LeaseError) -> WorkerError {
    Box::new(e)
}

#[async_trait::async_trait]
impl AsyncLeaseStore for DynamoDbLeaseStore {
    async fn get(&self, key: &str) -> Result<Option<LeaseView>, WorkerError> {
        Ok(DynamoDbLeaseStore::get(self, key)
            .await?
            .map(|l| LeaseView {
                completed: l.completed,
            }))
    }
    async fn list(
        &self,
    ) -> Result<Vec<amazon_dynamodb_streams_consumer_core::coordinator::RawLease>, WorkerError>
    {
        Ok(DynamoDbLeaseStore::list_all(self)
            .await?
            .into_iter()
            .map(
                |l| amazon_dynamodb_streams_consumer_core::coordinator::RawLease {
                    lease_key: l.lease_key,
                    owner: l.lease_owner,
                    lease_counter: l.lease_counter,
                    completed: l.completed,
                    checkpoint: l.checkpoint,
                    parents: l.parents,
                },
            )
            .collect())
    }
    async fn renew(&self, key: &str, owner: &str, counter: u64) -> Result<u64, WorkerError> {
        DynamoDbLeaseStore::renew(self, key, owner, counter)
            .await
            .map_err(box_lease)
    }
    async fn heartbeat(&self, worker: &str) -> Result<u64, WorkerError> {
        DynamoDbLeaseStore::heartbeat(self, worker)
            .await
            .map_err(box_lease)
    }
    async fn acquire(&self, key: &str, owner: &str) -> Result<LeaseHandle, WorkerError> {
        let l = DynamoDbLeaseStore::acquire(self, key, owner)
            .await
            .map_err(box_lease)?;
        Ok(LeaseHandle {
            owner: l.lease_owner.unwrap_or_default(),
            counter: l.lease_counter,
            checkpoint: l.checkpoint,
        })
    }
    async fn checkpoint(
        &self,
        key: &str,
        owner: &str,
        counter: u64,
        seq: &str,
    ) -> Result<u64, WorkerError> {
        DynamoDbLeaseStore::checkpoint(self, key, owner, counter, seq)
            .await
            .map_err(box_lease)
    }
    async fn mark_complete(&self, key: &str, owner: &str, counter: u64) -> Result<(), WorkerError> {
        DynamoDbLeaseStore::mark_complete(self, key, owner, counter)
            .await
            .map_err(box_lease)
    }
    async fn release(&self, key: &str, owner: &str, counter: u64) -> Result<(), WorkerError> {
        DynamoDbLeaseStore::release(self, key, owner, counter)
            .await
            .map_err(box_lease)
    }
    async fn delete_lease(&self, key: &str) -> Result<(), WorkerError> {
        DynamoDbLeaseStore::delete_lease(self, key)
            .await
            .map_err(box_lease)
    }
    async fn create_shard_lease(
        &self,
        key: &str,
        parents: &[String],
        checkpoint: Option<&str>,
    ) -> Result<(), WorkerError> {
        DynamoDbLeaseStore::create_shard_lease(self, key, parents, checkpoint)
            .await
            .map_err(box_lease)
    }
    async fn try_acquire_leadership(
        &self,
        key: &str,
        owner: &str,
        expected: Option<u64>,
    ) -> Result<Option<u64>, WorkerError> {
        DynamoDbLeaseStore::try_acquire_leadership(self, key, owner, expected)
            .await
            .map_err(box_lease)
    }
}
