#![cfg(feature = "aws")]
//! Live integration tests for lease-table auto-creation options (billing mode +
//! PITR). Skipped unless `DDB_STREAMS_CONSUMER_IT=1`. Each test creates and
//! deletes its own lease table.
//!
//! Run:
//!   DDB_STREAMS_CONSUMER_IT=1 cargo test -p amazon-dynamodb-streams-consumer-lease \
//!     --features aws --test live_table_config -- --nocapture

use amazon_dynamodb_streams_consumer_lease::dynamodb::{
    DynamoDbLeaseStore, LeaseBilling, LeaseTableConfig,
};
use aws_sdk_dynamodb as ddb;
use ddb::types::{BillingMode, PointInTimeRecoveryStatus};

fn gated() -> bool {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live table-config integ test (set DDB_STREAMS_CONSUMER_IT=1 to run)");
        return false;
    }
    true
}

/// Provisioned billing config must land verbatim on the created table:
/// BillingMode=PROVISIONED with the exact RCUs/WCUs requested.
#[tokio::test]
async fn live_provisioned_billing_applied_on_create() {
    if !gated() {
        return;
    }
    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = ddb::Client::new(&cfg);
    let table = format!(
        "amazon-dynamodb-streams-consumer-leases-prov-it-{}",
        std::process::id()
    );

    let store =
        DynamoDbLeaseStore::new(client.clone(), &table).with_table_config(LeaseTableConfig {
            billing: LeaseBilling::Provisioned {
                read_capacity: 7,
                write_capacity: 9,
            },
            pitr: false,
        });
    store.ensure_table().await.expect("ensure_table");

    let described = client.describe_table().table_name(&table).send().await;
    let outcome = described.map(|resp| {
        let t = resp.table().expect("table desc").clone();
        (
            t.billing_mode_summary()
                .and_then(|b| b.billing_mode())
                .cloned(),
            t.provisioned_throughput()
                .and_then(|p| p.read_capacity_units()),
            t.provisioned_throughput()
                .and_then(|p| p.write_capacity_units()),
        )
    });

    // Best-effort cleanup regardless of assertion outcome.
    let _ = client.delete_table().table_name(&table).send().await;

    let (billing, rcu, wcu) = outcome.expect("describe_table");
    // RCUs/WCUs are the definitive proof: a PAY_PER_REQUEST table reports 0/0,
    // so 7/9 can only come from the PROVISIONED billing we requested. DynamoDB
    // omits BillingModeSummary for PROVISIONED tables, so only assert it is not
    // PAY_PER_REQUEST when present.
    assert_eq!(rcu, Some(7), "read capacity units");
    assert_eq!(wcu, Some(9), "write capacity units");
    assert_ne!(
        billing,
        Some(BillingMode::PayPerRequest),
        "billing must not be on-demand"
    );
    eprintln!("provisioned billing OK: rcu=7 wcu=9 billing_mode_summary={billing:?}");
}

/// PITR must be ENABLED on a freshly-created lease table when requested.
#[tokio::test]
async fn live_pitr_enabled_on_create() {
    if !gated() {
        return;
    }
    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = ddb::Client::new(&cfg);
    let table = format!(
        "amazon-dynamodb-streams-consumer-leases-pitr-it-{}",
        std::process::id()
    );

    let store =
        DynamoDbLeaseStore::new(client.clone(), &table).with_table_config(LeaseTableConfig {
            billing: LeaseBilling::PayPerRequest,
            pitr: true,
        });
    store.ensure_table().await.expect("ensure_table");

    let described = client
        .describe_continuous_backups()
        .table_name(&table)
        .send()
        .await;
    let status = described.map(|resp| {
        resp.continuous_backups_description()
            .and_then(|c| c.point_in_time_recovery_description())
            .and_then(|p| p.point_in_time_recovery_status())
            .cloned()
    });

    // Best-effort cleanup regardless of assertion outcome.
    let _ = client.delete_table().table_name(&table).send().await;

    let status = status.expect("describe_continuous_backups");
    assert_eq!(
        status,
        Some(PointInTimeRecoveryStatus::Enabled),
        "PITR status on freshly-created lease table"
    );
    eprintln!("PITR OK: enabled on freshly-created lease table");
}
