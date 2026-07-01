#![cfg(feature = "aws")]
//! Live multi-worker lease-steal test. Skipped unless `DDBSTREAMS_KCL_IT=1`.
//!
//! Scenario: w1 acquires 4 leases, stays alive on 2 (renews) and "dies" on 2.
//! w2 runs the pure taker over a snapshot (2 leases flagged expired), then
//! actually claims them via the DynamoDB lease store. Asserts w2 wins the two
//! stolen leases and w1 can no longer renew one (optimistic lock). Creates and
//! deletes its own lease table.
//!
//! Run:
//!   DDBSTREAMS_KCL_IT=1 cargo test -p ddbstreams-kcl-lease-dynamodb \
//!     --features aws --test live_steal -- --nocapture

use aws_sdk_dynamodb as ddb;
use ddbstreams_kcl_core::taker::{compute_leases_to_take, LeaseSnapshot};
use ddbstreams_kcl_lease_dynamodb::dynamodb::{DynamoDbLeaseStore, LeaseError};

#[tokio::test]
async fn live_worker_steals_expired_leases() {
    if std::env::var("DDBSTREAMS_KCL_IT").is_err() {
        eprintln!("skipping live steal test (set DDBSTREAMS_KCL_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = ddb::Client::new(&cfg);
    let table = format!("ddbstreams-kcl-steal-it-{}", std::process::id());
    let store = DynamoDbLeaseStore::new(client.clone(), &table);
    store.ensure_table().await.expect("ensure_table");

    let outcome = run_steal(&store).await;
    let _ = client.delete_table().table_name(&table).send().await;
    outcome.expect("steal scenario");
}

async fn run_steal(store: &DynamoDbLeaseStore) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // w1 acquires 4 leases (counter 1 each).
    for k in ["s0", "s1", "s2", "s3"] {
        let l = store.acquire(k, "w1").await.map_err(bx)?;
        assert_eq!(l.lease_counter, 1);
    }
    // w1 stays alive on s0,s1 (renew → counter 2); "dies" on s2,s3.
    store.renew("s0", "w1", 1).await.map_err(bx)?;
    store.renew("s1", "w1", 1).await.map_err(bx)?;

    // Coordinator judgment: s2,s3 expired (w1 stopped heartbeating). Build the
    // snapshot the taker sees.
    let snapshot = vec![
        LeaseSnapshot { lease_key: "s0".into(), owner: Some("w1".into()), expired: false, completed: false },
        LeaseSnapshot { lease_key: "s1".into(), owner: Some("w1".into()), expired: false, completed: false },
        LeaseSnapshot { lease_key: "s2".into(), owner: Some("w1".into()), expired: true, completed: false },
        LeaseSnapshot { lease_key: "s3".into(), owner: Some("w1".into()), expired: true, completed: false },
    ];

    // Pure taker: w2 (holds 0, target 2) should take the two expired leases.
    let mut to_take = compute_leases_to_take(&snapshot, "w2", 10);
    to_take.sort();
    assert_eq!(to_take, vec!["s2".to_string(), "s3".to_string()]);

    // w2 actually claims them via the store (conditional on current counter).
    for k in &to_take {
        let l = store.acquire(k, "w2").await.map_err(bx)?;
        assert_eq!(l.lease_owner.as_deref(), Some("w2"));
    }

    // w2 now owns s2; w1 trying to renew s2 at its stale counter must lose.
    let s2 = store.get("s2").await?.expect("s2");
    assert_eq!(s2.lease_owner.as_deref(), Some("w2"));
    match store.renew("s2", "w1", 1).await {
        Err(LeaseError::Lost) => {}
        other => return Err(format!("expected w1 renew of stolen s2 to be Lost, got {other:?}").into()),
    }

    // w1 still holds s0 (renewable at counter 2).
    let c = store.renew("s0", "w1", 2).await.map_err(bx)?;
    assert_eq!(c, 3);

    eprintln!("steal OK: w2 took s2,s3 (taker-selected); w1 lost s2, kept s0");
    Ok(())
}

fn bx(e: LeaseError) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(e)
}
