//! `amazon-dynamodb-streams-consumer-sidecar` — the JVM-free consumer process a language binding
//! spawns and talks to over stdio.
//!
//! Responsibilities: run the full Rust KCL consumer (shard discovery, DynamoDB
//! leases, per-shard ordered reads, lease balancing, checkpoints) and stream
//! ordered record batches to the client on **stdout** using the JSON-Lines
//! protocol, checkpointing only when the client acks on **stdin**. All logging
//! goes to **stderr** so it never corrupts the protocol channel.
//!
//! Config (environment):
//!   DDB_STREAMS_CONSUMER_STREAM_ARN      (required) DynamoDB Streams ARN
//!   DDB_STREAMS_CONSUMER_LEASE_TABLE     (required) DynamoDB lease table name
//!   DDB_STREAMS_CONSUMER_OWNER           lease owner id (default `<host>:<pid>`)
//!   DDB_STREAMS_CONSUMER_MAX_LEASES      max leases this worker holds (default 100)
//!   DDB_STREAMS_CONSUMER_LEASE_DURATION_MS   lease expiry (default 10000)
//!   DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS    per-shard idle poll backoff (default 1000)
//!   DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS   sleep between coordination cycles (default 1000)
//!   DDB_STREAMS_CONSUMER_INITIAL_POSITION    start position for freshly-seeded
//!                                            shards: TRIM_HORIZON (default) or LATEST
//!   DDB_STREAMS_CONSUMER_GRACEFUL_SHUTDOWN_MS window to let processors flush on
//!                                            shutdown before releasing leases (default 5000)
//!   DDB_STREAMS_CONSUMER_MAX_RECORDS         GetRecords batch-size limit 1..=1000
//!                                            (default: service default)
//!   DDB_STREAMS_CONSUMER_LEASE_BILLING_MODE  lease-table billing when auto-created:
//!                                            PAY_PER_REQUEST (default) or PROVISIONED
//!   DDB_STREAMS_CONSUMER_LEASE_READ_CAPACITY  provisioned RCUs (default 5, PROVISIONED only)
//!   DDB_STREAMS_CONSUMER_LEASE_WRITE_CAPACITY provisioned WCUs (default 5, PROVISIONED only)
//!   DDB_STREAMS_CONSUMER_LEASE_PITR          enable PITR on the auto-created lease
//!                                            table (default false; needs
//!                                            dynamodb:UpdateContinuousBackups)
//!   AWS_REGION / standard AWS env  used by the SDK for creds + region

mod ipc;
#[cfg(feature = "otel")]
mod otel;

use amazon_dynamodb_streams_consumer_core::coordinator::LeaseCoordinator;
use amazon_dynamodb_streams_consumer_core::InitialPosition;
use amazon_dynamodb_streams_consumer_core::ShardId;
use amazon_dynamodb_streams_consumer_lease::dynamodb::{
    DynamoDbLeaseStore, LeaseBilling, LeaseTableConfig,
};
use amazon_dynamodb_streams_consumer_source::aws::DdbStreamsSource;
use amazon_dynamodb_streams_consumer_worker::fleet::{Fleet, FleetConfig, Leadership};
use amazon_dynamodb_streams_consumer_worker::{AsyncLeaseStore, AsyncStreamSource, WorkerError};
use ipc::{Ipc, IpcConsumerFactory};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The lease side of a graceful handoff: which shards this worker still owns,
/// and releasing them. Abstracted over [`Fleet`] so the handoff ordering can be
/// unit tested without a real source/lease store.
#[async_trait::async_trait]
trait LeaseHandoff {
    async fn list_owned(&self) -> Result<Vec<ShardId>, WorkerError>;
    async fn release_all_owned(&self) -> Result<usize, WorkerError>;
}

#[async_trait::async_trait]
impl<S, L> LeaseHandoff for Fleet<S, L>
where
    S: AsyncStreamSource + Send + Sync + 'static,
    L: AsyncLeaseStore + Send + Sync + 'static,
{
    async fn list_owned(&self) -> Result<Vec<ShardId>, WorkerError> {
        self.owned_shards().await
    }
    async fn release_all_owned(&self) -> Result<usize, WorkerError> {
        self.release_owned().await
    }
}

/// The client-notification side of a graceful handoff. Abstracted over [`Ipc`]
/// so the handoff ordering can be unit tested without a real stdio client.
#[async_trait::async_trait]
trait ShutdownNotifier {
    async fn notify_shutdown(&self, shard: &str);
}

#[async_trait::async_trait]
impl ShutdownNotifier for Ipc {
    async fn notify_shutdown(&self, shard: &str) {
        self.shutdown_requested(shard).await;
    }
}

/// Graceful lease handoff (ordering-critical): notify the client for EACH owned
/// shard so its processor can flush/close, wait a bounded window, and only THEN
/// release the leases so another worker takes over immediately (vs waiting for
/// expiry). Every notification MUST precede the release — that is what lets a
/// processor commit its last acked position before the lease moves. If the
/// owned-shard lookup fails we skip notifications but still release, so leases
/// are never stranded. Extracted from `main` so the ordering is unit tested.
async fn graceful_handoff<H, N>(handoff: &H, notifier: &N, graceful_shutdown_timeout_ms: u64)
where
    H: LeaseHandoff + ?Sized,
    N: ShutdownNotifier + ?Sized,
{
    match handoff.list_owned().await {
        Ok(owned) if !owned.is_empty() => {
            for shard in &owned {
                notifier.notify_shutdown(shard).await;
            }
            // Give processors a bounded window to flush before the lease moves.
            tokio::time::sleep(Duration::from_millis(graceful_shutdown_timeout_ms)).await;
        }
        Ok(_) => {}
        Err(e) => eprintln!("[sidecar] owned-shards lookup error: {e}"),
    }
    match handoff.release_all_owned().await {
        Ok(n) => eprintln!("[sidecar] released {n} lease(s)"),
        Err(e) => eprintln!("[sidecar] lease release error: {e}"),
    }
}

struct Config {
    stream_arn: String,
    lease_table: String,
    owner: String,
    max_leases: usize,
    lease_duration_ms: u64,
    poll_interval_ms: u64,
    cycle_interval_ms: u64,
    initial_position: InitialPosition,
    graceful_shutdown_timeout_ms: u64,
    /// Optional GetRecords batch-size limit (None = service default).
    max_records: Option<i32>,
    /// Optional cap on the number of shards processed concurrently
    /// (None = unbounded, one processing slot per shard).
    max_processing_concurrency: Option<usize>,
    /// Billing mode + PITR applied when the lease table is auto-created.
    lease_table_config: LeaseTableConfig,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let req = |k: &str| std::env::var(k).map_err(|_| format!("missing required env var {k}"));
        let opt_u64 = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let owner = std::env::var("DDB_STREAMS_CONSUMER_OWNER").unwrap_or_else(|_| {
            let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".into());
            format!("{host}:{}", std::process::id())
        });
        let initial_position = match std::env::var("DDB_STREAMS_CONSUMER_INITIAL_POSITION") {
            Ok(v) => InitialPosition::parse(&v)
                .ok_or_else(|| format!("invalid DDB_STREAMS_CONSUMER_INITIAL_POSITION: {v}"))?,
            Err(_) => InitialPosition::default(),
        };
        let max_records = std::env::var("DDB_STREAMS_CONSUMER_MAX_RECORDS")
            .ok()
            .and_then(|v| v.parse::<i32>().ok());
        // Optional processing-concurrency cap. Absent, unparseable, or 0 => None
        // (unbounded), matching Fleet::with_max_processing_concurrency semantics.
        let max_processing_concurrency =
            std::env::var("DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| n >= 1);
        let lease_table_config = {
            let billing = match std::env::var("DDB_STREAMS_CONSUMER_LEASE_BILLING_MODE")
                .unwrap_or_default()
                .to_ascii_uppercase()
                .as_str()
            {
                "PROVISIONED" => LeaseBilling::Provisioned {
                    read_capacity: opt_u64("DDB_STREAMS_CONSUMER_LEASE_READ_CAPACITY", 5) as i64,
                    write_capacity: opt_u64("DDB_STREAMS_CONSUMER_LEASE_WRITE_CAPACITY", 5) as i64,
                },
                // "", "PAY_PER_REQUEST", or anything else -> on-demand default.
                _ => LeaseBilling::PayPerRequest,
            };
            let pitr = std::env::var("DDB_STREAMS_CONSUMER_LEASE_PITR")
                .map(|v| {
                    matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(false);
            LeaseTableConfig { billing, pitr }
        };
        Ok(Self {
            stream_arn: req("DDB_STREAMS_CONSUMER_STREAM_ARN")?,
            lease_table: req("DDB_STREAMS_CONSUMER_LEASE_TABLE")?,
            owner,
            max_leases: opt_u64("DDB_STREAMS_CONSUMER_MAX_LEASES", 100) as usize,
            lease_duration_ms: opt_u64("DDB_STREAMS_CONSUMER_LEASE_DURATION_MS", 10_000),
            poll_interval_ms: opt_u64("DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS", 1_000),
            cycle_interval_ms: opt_u64("DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS", 1_000),
            initial_position,
            graceful_shutdown_timeout_ms: opt_u64(
                "DDB_STREAMS_CONSUMER_GRACEFUL_SHUTDOWN_MS",
                5_000,
            ),
            max_records,
            max_processing_concurrency,
            lease_table_config,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[sidecar] config error: {e}");
            std::process::exit(2);
        }
    };
    eprintln!(
        "[sidecar] starting owner={} stream={} lease_table={} max_leases={} lease_ms={}",
        cfg.owner, cfg.stream_arn, cfg.lease_table, cfg.max_leases, cfg.lease_duration_ms
    );

    // Live AWS wiring.
    let mut source = DdbStreamsSource::from_env(&cfg.stream_arn).await;
    if let Some(n) = cfg.max_records {
        source = source.with_max_records(n);
    }
    let leases = DynamoDbLeaseStore::from_env(&cfg.lease_table)
        .await
        .with_table_config(cfg.lease_table_config.clone());
    leases.ensure_table().await?;

    // Stdio IPC to the client.
    let ipc = Ipc::new(tokio::io::stdout());
    ipc.spawn_reader(tokio::io::stdin());

    // Graceful stop on SIGINT/SIGTERM.
    {
        let ipc = ipc.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("[sidecar] signal received, stopping");
            ipc.request_stop();
        });
    }

    let factory = Arc::new(IpcConsumerFactory::new(ipc.clone()));
    #[cfg_attr(not(feature = "otel"), allow(unused_mut))]
    let mut fleet = Fleet::new(
        source,
        leases,
        factory,
        FleetConfig {
            owner: cfg.owner.clone(),
            max_leases: cfg.max_leases,
            lease_duration_ms: cfg.lease_duration_ms,
            poll_interval_ms: cfg.poll_interval_ms,
            initial_position: cfg.initial_position,
        },
    )
    .with_max_processing_concurrency(cfg.max_processing_concurrency);

    // Model A: if the standard OTEL exporter env var is set, attach the OTLP
    // metrics sink (feature-gated so the default build has no OTEL deps).
    #[cfg(feature = "otel")]
    let mut otel_sink: Option<std::sync::Arc<otel::OtelMetricsSink>> = None;
    #[cfg(feature = "otel")]
    {
        if std::env::var("OTEL_METRICS_EXPORTER").is_ok() {
            match otel::OtelMetricsSink::from_env().await {
                Ok(sink) => {
                    let sink = std::sync::Arc::new(sink);
                    fleet = fleet.with_metrics(sink.clone());
                    otel_sink = Some(sink);
                    eprintln!("[sidecar] OTLP metrics enabled");
                }
                Err(e) => eprintln!("[sidecar] OTLP metrics init failed: {e}"),
            }
        }
    }

    // Continuous coordination loop: each cycle scans leases, rebalances, and
    // runs one concurrent task per owned shard (which streams batches to the
    // client and checkpoints on ack). Runs until the client stops, a signal
    // arrives, or every shard's lease completes.
    let mut coordinator =
        LeaseCoordinator::new(cfg.owner.clone(), cfg.max_leases, cfg.lease_duration_ms);
    let mut leadership = Leadership::new(cfg.owner.clone(), cfg.lease_duration_ms);
    let start = Instant::now();
    loop {
        if ipc.is_stopped() {
            break;
        }
        let now_ms = start.elapsed().as_millis() as u64;
        match fleet
            .run_cycle(&mut coordinator, &mut leadership, now_ms)
            .await
        {
            Ok(true) => {
                eprintln!("[sidecar] all shards complete");
                break;
            }
            Ok(false) => {}
            Err(e) => eprintln!("[sidecar] cycle error: {e}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(cfg.cycle_interval_ms)) => {}
            _ = ipc.stopped() => break,
        }
    }

    // Graceful handoff: notify each owned shard so its processor can flush/close,
    // wait a bounded window, THEN release the leases (see `graceful_handoff`).
    graceful_handoff(&fleet, &*ipc, cfg.graceful_shutdown_timeout_ms).await;

    // Flush any buffered metrics before exit so the final interval isn't lost.
    #[cfg(feature = "otel")]
    if let Some(s) = &otel_sink {
        match s.force_flush() {
            Ok(_) => eprintln!("[sidecar] flushed final metrics"),
            Err(e) => eprintln!("[sidecar] metrics flush error: {e}"),
        }
    }
    ipc.shutdown("sidecar stopping").await;
    eprintln!("[sidecar] stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{graceful_handoff, Config, LeaseBilling, LeaseHandoff, ShutdownNotifier};
    use amazon_dynamodb_streams_consumer_core::ShardId;
    use amazon_dynamodb_streams_consumer_worker::WorkerError;
    use std::sync::{Arc, Mutex};

    // Env is process-global; serialize the env-touching tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const VARS: &[&str] = &[
        "DDB_STREAMS_CONSUMER_STREAM_ARN",
        "DDB_STREAMS_CONSUMER_LEASE_TABLE",
        "DDB_STREAMS_CONSUMER_OWNER",
        "DDB_STREAMS_CONSUMER_MAX_LEASES",
        "DDB_STREAMS_CONSUMER_LEASE_DURATION_MS",
        "DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS",
        "DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS",
        "DDB_STREAMS_CONSUMER_INITIAL_POSITION",
        "DDB_STREAMS_CONSUMER_GRACEFUL_SHUTDOWN_MS",
        "DDB_STREAMS_CONSUMER_MAX_RECORDS",
        "DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY",
        "DDB_STREAMS_CONSUMER_LEASE_BILLING_MODE",
        "DDB_STREAMS_CONSUMER_LEASE_READ_CAPACITY",
        "DDB_STREAMS_CONSUMER_LEASE_WRITE_CAPACITY",
        "DDB_STREAMS_CONSUMER_LEASE_PITR",
        "HOSTNAME",
    ];

    fn clear() {
        for v in VARS {
            std::env::remove_var(v);
        }
    }

    #[test]
    fn missing_required_is_an_error() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        assert!(Config::from_env().is_err(), "no stream arn → error");
        std::env::set_var("DDB_STREAMS_CONSUMER_STREAM_ARN", "arn");
        assert!(Config::from_env().is_err(), "no lease table → error");
        clear();
    }

    #[test]
    fn defaults_and_owner_fallback() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        std::env::set_var("DDB_STREAMS_CONSUMER_STREAM_ARN", "arn");
        std::env::set_var("DDB_STREAMS_CONSUMER_LEASE_TABLE", "leases");
        std::env::set_var("HOSTNAME", "host7");
        let c = Config::from_env().unwrap();
        assert_eq!(c.max_leases, 100);
        assert_eq!(c.lease_duration_ms, 10_000);
        assert_eq!(c.poll_interval_ms, 1_000);
        assert_eq!(c.cycle_interval_ms, 1_000);
        assert!(
            c.owner.starts_with("host7:"),
            "owner defaults to <host>:<pid>, got {}",
            c.owner
        );
        // Knob defaults: no explicit GetRecords limit, on-demand billing, no PITR.
        assert_eq!(c.max_records, None);
        assert_eq!(c.max_processing_concurrency, None);
        assert_eq!(c.lease_table_config.billing, LeaseBilling::PayPerRequest);
        assert!(!c.lease_table_config.pitr);
        clear();
    }

    #[test]
    fn explicit_values_are_parsed() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        std::env::set_var("DDB_STREAMS_CONSUMER_STREAM_ARN", "arn");
        std::env::set_var("DDB_STREAMS_CONSUMER_LEASE_TABLE", "leases");
        std::env::set_var("DDB_STREAMS_CONSUMER_OWNER", "worker-9");
        std::env::set_var("DDB_STREAMS_CONSUMER_MAX_LEASES", "5");
        std::env::set_var("DDB_STREAMS_CONSUMER_LEASE_DURATION_MS", "3000");
        std::env::set_var("DDB_STREAMS_CONSUMER_POLL_INTERVAL_MS", "200");
        std::env::set_var("DDB_STREAMS_CONSUMER_CYCLE_INTERVAL_MS", "250");
        std::env::set_var("DDB_STREAMS_CONSUMER_MAX_RECORDS", "250");
        std::env::set_var("DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY", "4");
        std::env::set_var("DDB_STREAMS_CONSUMER_LEASE_BILLING_MODE", "provisioned");
        std::env::set_var("DDB_STREAMS_CONSUMER_LEASE_READ_CAPACITY", "7");
        std::env::set_var("DDB_STREAMS_CONSUMER_LEASE_WRITE_CAPACITY", "9");
        std::env::set_var("DDB_STREAMS_CONSUMER_LEASE_PITR", "true");
        let c = Config::from_env().unwrap();
        assert_eq!(c.owner, "worker-9");
        assert_eq!(c.max_leases, 5);
        assert_eq!(c.max_processing_concurrency, Some(4));
        assert_eq!(c.lease_duration_ms, 3000);
        assert_eq!(c.poll_interval_ms, 200);
        assert_eq!(c.cycle_interval_ms, 250);
        assert_eq!(c.max_records, Some(250));
        assert_eq!(
            c.lease_table_config.billing,
            LeaseBilling::Provisioned {
                read_capacity: 7,
                write_capacity: 9,
            }
        );
        assert!(c.lease_table_config.pitr);
        clear();
    }

    // ---- graceful_handoff ordering ----
    // A shared, ordered event log proves the ordering contract: every owned
    // shard is notified BEFORE the leases are released.
    #[derive(Clone, PartialEq, Debug)]
    enum Ev {
        Notify(String),
        Release,
    }

    struct FakeHandoff {
        owned: Vec<ShardId>,
        fail_lookup: bool,
        log: Arc<Mutex<Vec<Ev>>>,
    }
    #[async_trait::async_trait]
    impl LeaseHandoff for FakeHandoff {
        async fn list_owned(&self) -> Result<Vec<ShardId>, WorkerError> {
            if self.fail_lookup {
                return Err("owned-shards lookup failed".into());
            }
            Ok(self.owned.clone())
        }
        async fn release_all_owned(&self) -> Result<usize, WorkerError> {
            self.log.lock().unwrap().push(Ev::Release);
            Ok(self.owned.len())
        }
    }

    struct FakeNotifier {
        log: Arc<Mutex<Vec<Ev>>>,
    }
    #[async_trait::async_trait]
    impl ShutdownNotifier for FakeNotifier {
        async fn notify_shutdown(&self, shard: &str) {
            self.log.lock().unwrap().push(Ev::Notify(shard.to_string()));
        }
    }

    #[tokio::test]
    async fn handoff_notifies_every_owned_shard_before_release() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let h = FakeHandoff {
            owned: vec!["s0".into(), "s1".into(), "s2".into()],
            fail_lookup: false,
            log: log.clone(),
        };
        let n = FakeNotifier { log: log.clone() };

        graceful_handoff(&h, &n, 0).await;

        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                Ev::Notify("s0".into()),
                Ev::Notify("s1".into()),
                Ev::Notify("s2".into()),
                Ev::Release,
            ],
            "all owned shards must be notified, in order, before release"
        );
    }

    #[tokio::test]
    async fn handoff_releases_without_notifying_when_no_shards_owned() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let h = FakeHandoff {
            owned: vec![],
            fail_lookup: false,
            log: log.clone(),
        };
        let n = FakeNotifier { log: log.clone() };

        graceful_handoff(&h, &n, 0).await;

        // No shards owned → no notifications, but leases are still released.
        assert_eq!(log.lock().unwrap().clone(), vec![Ev::Release]);
    }

    #[tokio::test]
    async fn handoff_still_releases_when_owned_lookup_fails() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let h = FakeHandoff {
            owned: vec!["s0".into()],
            fail_lookup: true,
            log: log.clone(),
        };
        let n = FakeNotifier { log: log.clone() };

        graceful_handoff(&h, &n, 0).await;

        // Lookup failed → skip notifications, but never strand the leases.
        assert_eq!(log.lock().unwrap().clone(), vec![Ev::Release]);
    }
}
