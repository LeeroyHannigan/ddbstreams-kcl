//! `ddbstreams-kcl-sidecar` — the JVM-free consumer process a language binding
//! spawns and talks to over stdio.
//!
//! Responsibilities: run the full Rust KCL consumer (shard discovery, DynamoDB
//! leases, per-shard ordered reads, lease balancing, checkpoints) and stream
//! ordered record batches to the client on **stdout** using the JSON-Lines
//! protocol, checkpointing only when the client acks on **stdin**. All logging
//! goes to **stderr** so it never corrupts the protocol channel.
//!
//! Config (environment):
//!   DDBSTREAMS_KCL_STREAM_ARN      (required) DynamoDB Streams ARN
//!   DDBSTREAMS_KCL_LEASE_TABLE     (required) DynamoDB lease table name
//!   DDBSTREAMS_KCL_OWNER           lease owner id (default `<host>:<pid>`)
//!   DDBSTREAMS_KCL_MAX_LEASES      max leases this worker holds (default 100)
//!   DDBSTREAMS_KCL_LEASE_DURATION_MS   lease expiry (default 10000)
//!   DDBSTREAMS_KCL_POLL_INTERVAL_MS    per-shard idle poll backoff (default 1000)
//!   DDBSTREAMS_KCL_CYCLE_INTERVAL_MS   sleep between coordination cycles (default 1000)
//!   AWS_REGION / standard AWS env  used by the SDK for creds + region

mod ipc;

use ddbstreams_kcl_core::coordinator::LeaseCoordinator;
use ddbstreams_kcl_lease_dynamodb::dynamodb::DynamoDbLeaseStore;
use ddbstreams_kcl_source_ddbstreams::aws::DdbStreamsSource;
use ddbstreams_kcl_worker::fleet::{Fleet, FleetConfig};
use ipc::{Ipc, IpcConsumerFactory};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Config {
    stream_arn: String,
    lease_table: String,
    owner: String,
    max_leases: usize,
    lease_duration_ms: u64,
    poll_interval_ms: u64,
    cycle_interval_ms: u64,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let req = |k: &str| std::env::var(k).map_err(|_| format!("missing required env var {k}"));
        let opt_u64 = |k: &str, d: u64| {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        let owner = std::env::var("DDBSTREAMS_KCL_OWNER").unwrap_or_else(|_| {
            let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".into());
            format!("{host}:{}", std::process::id())
        });
        Ok(Self {
            stream_arn: req("DDBSTREAMS_KCL_STREAM_ARN")?,
            lease_table: req("DDBSTREAMS_KCL_LEASE_TABLE")?,
            owner,
            max_leases: opt_u64("DDBSTREAMS_KCL_MAX_LEASES", 100) as usize,
            lease_duration_ms: opt_u64("DDBSTREAMS_KCL_LEASE_DURATION_MS", 10_000),
            poll_interval_ms: opt_u64("DDBSTREAMS_KCL_POLL_INTERVAL_MS", 1_000),
            cycle_interval_ms: opt_u64("DDBSTREAMS_KCL_CYCLE_INTERVAL_MS", 1_000),
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
    let source = DdbStreamsSource::from_env(&cfg.stream_arn).await;
    let leases = DynamoDbLeaseStore::from_env(&cfg.lease_table).await;
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
    let fleet = Fleet::new(
        source,
        leases,
        factory,
        FleetConfig {
            owner: cfg.owner.clone(),
            max_leases: cfg.max_leases,
            lease_duration_ms: cfg.lease_duration_ms,
            poll_interval_ms: cfg.poll_interval_ms,
        },
    );

    // Continuous coordination loop: each cycle scans leases, rebalances, and
    // runs one concurrent task per owned shard (which streams batches to the
    // client and checkpoints on ack). Runs until the client stops, a signal
    // arrives, or every shard's lease completes.
    let mut coordinator =
        LeaseCoordinator::new(cfg.owner.clone(), cfg.max_leases, cfg.lease_duration_ms);
    let start = Instant::now();
    loop {
        if ipc.is_stopped() {
            break;
        }
        let now_ms = start.elapsed().as_millis() as u64;
        match fleet.run_cycle(&mut coordinator, now_ms).await {
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

    // Graceful shutdown: release our leases so another worker takes over
    // immediately instead of waiting for expiry.
    match fleet.release_owned().await {
        Ok(n) => eprintln!("[sidecar] released {n} lease(s)"),
        Err(e) => eprintln!("[sidecar] lease release error: {e}"),
    }
    ipc.shutdown("sidecar stopping").await;
    eprintln!("[sidecar] stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Config;
    use std::sync::Mutex;

    // Env is process-global; serialize the env-touching tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const VARS: &[&str] = &[
        "DDBSTREAMS_KCL_STREAM_ARN",
        "DDBSTREAMS_KCL_LEASE_TABLE",
        "DDBSTREAMS_KCL_OWNER",
        "DDBSTREAMS_KCL_MAX_LEASES",
        "DDBSTREAMS_KCL_LEASE_DURATION_MS",
        "DDBSTREAMS_KCL_POLL_INTERVAL_MS",
        "DDBSTREAMS_KCL_CYCLE_INTERVAL_MS",
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
        std::env::set_var("DDBSTREAMS_KCL_STREAM_ARN", "arn");
        assert!(Config::from_env().is_err(), "no lease table → error");
        clear();
    }

    #[test]
    fn defaults_and_owner_fallback() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        std::env::set_var("DDBSTREAMS_KCL_STREAM_ARN", "arn");
        std::env::set_var("DDBSTREAMS_KCL_LEASE_TABLE", "leases");
        std::env::set_var("HOSTNAME", "host7");
        let c = Config::from_env().unwrap();
        assert_eq!(c.max_leases, 100);
        assert_eq!(c.lease_duration_ms, 10_000);
        assert_eq!(c.poll_interval_ms, 1_000);
        assert_eq!(c.cycle_interval_ms, 1_000);
        assert!(c.owner.starts_with("host7:"), "owner defaults to <host>:<pid>, got {}", c.owner);
        clear();
    }

    #[test]
    fn explicit_values_are_parsed() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        std::env::set_var("DDBSTREAMS_KCL_STREAM_ARN", "arn");
        std::env::set_var("DDBSTREAMS_KCL_LEASE_TABLE", "leases");
        std::env::set_var("DDBSTREAMS_KCL_OWNER", "worker-9");
        std::env::set_var("DDBSTREAMS_KCL_MAX_LEASES", "5");
        std::env::set_var("DDBSTREAMS_KCL_LEASE_DURATION_MS", "3000");
        std::env::set_var("DDBSTREAMS_KCL_POLL_INTERVAL_MS", "200");
        std::env::set_var("DDBSTREAMS_KCL_CYCLE_INTERVAL_MS", "250");
        let c = Config::from_env().unwrap();
        assert_eq!(c.owner, "worker-9");
        assert_eq!(c.max_leases, 5);
        assert_eq!(c.lease_duration_ms, 3000);
        assert_eq!(c.poll_interval_ms, 200);
        assert_eq!(c.cycle_interval_ms, 250);
        clear();
    }
}
