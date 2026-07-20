//! OpenTelemetry (OTLP/HTTP) metrics sink — "Model A". Exports metrics over
//! **OTLP/HTTP** to whatever endpoint the customer configures, and can target
//! Amazon CloudWatch's **native OTLP metrics endpoint** directly (no collector):
//! `https://monitoring.<region>.amazonaws.com/v1/metrics` (GA 2026), which is
//! HTTP-only and SigV4-authenticated.
//!
//! Feature-gated (`otel`) so the default build carries no OpenTelemetry deps.
//!
//! Config (standard OTEL env):
//! - `OTEL_METRICS_EXPORTER=otlp` — enable
//! - `OTEL_EXPORTER_OTLP_ENDPOINT` — collector URL, or the CloudWatch endpoint
//! - `OTEL_EXPORTER_OTLP_HEADERS` — static headers (non-AWS backends)
//! - `OTEL_METRIC_EXPORT_INTERVAL` — flush interval ms (default 10000)
//!
//! When the endpoint host ends in `amazonaws.com`, requests are **SigV4-signed**
//! for service `monitoring` using the ambient AWS credentials — so a customer
//! points at the CloudWatch endpoint and gets metrics with no collector and no
//! bespoke CloudWatch sink.
//!
//! Emits the KCL/KCA-parity signals. Every metric carries `worker` and `stream`
//! dimensions (from `DDB_STREAMS_CONSUMER_OWNER` / `_STREAM_ARN`), matching KCL's
//! WorkerIdentifier/StreamId; shard-scoped metrics add `shard_id`:
//!
//! - `ddbstreams.consumer.millis_behind_latest` (gauge, ms) — consumer lag
//! - `ddbstreams.consumer.records_processed` (counter)
//! - `ddbstreams.consumer.bytes_processed` (counter)
//! - `ddbstreams.consumer.describe_stream.count` (counter)
//! - `ddbstreams.consumer.shard_end.count` (counter)
//! - `ddbstreams.consumer.lease.acquired` / `.lease.lost` (counters)
//! - `ddbstreams.consumer.lease.held` (gauge) — leases held by this worker

use amazon_dynamodb_streams_consumer_core::metrics::{MetricsSink, ShardMetrics};
use opentelemetry::metrics::{Counter, Gauge, Meter, MeterProvider};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::time::Duration;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct OtelMetricsSink {
    _provider: SdkMeterProvider,
    lag: Gauge<i64>,
    records: Counter<u64>,
    bytes: Counter<u64>,
    describes: Counter<u64>,
    shard_ends: Counter<u64>,
    lease_acquired: Counter<u64>,
    lease_lost: Counter<u64>,
    leases_held: Gauge<u64>,
    slot_wait: Gauge<u64>,
    max_concurrency: Gauge<u64>,
    /// Dimensions applied to every metric (worker id, stream) so load can be
    /// attributed per host/stream, matching KCL's WorkerIdentifier/StreamId.
    base_attrs: Vec<KeyValue>,
}

impl OtelMetricsSink {
    /// Build an OTLP/HTTP-exporting sink from the ambient `OTEL_*` environment.
    /// SigV4-signs requests when the endpoint targets an `amazonaws.com` host
    /// (e.g. the CloudWatch OTLP endpoint).
    pub async fn from_env() -> Result<Self, BoxError> {
        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4318".to_string());
        // OTLP/HTTP metrics path is `/v1/metrics`.
        let metrics_endpoint = if endpoint.ends_with("/v1/metrics") {
            endpoint.clone()
        } else {
            format!("{}/v1/metrics", endpoint.trim_end_matches('/'))
        };

        let sign_sigv4 = reqwest::Url::parse(&metrics_endpoint)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.ends_with("amazonaws.com")))
            .unwrap_or(false);

        let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let region = cfg
            .region()
            .map(|r| r.to_string())
            .unwrap_or_else(|| "us-east-1".to_string());
        let creds = cfg.credentials_provider();

        let http_client = sigv4::SigV4HttpClient {
            inner: reqwest::Client::new(),
            creds,
            region,
            service: "monitoring".to_string(),
            sign: sign_sigv4,
        };

        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_http_client(http_client)
            .with_endpoint(metrics_endpoint)
            .build()?;

        let interval_ms = std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10_000);
        let reader = PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_interval(Duration::from_millis(interval_ms))
            .build();
        // CloudWatch's OTLP endpoint files metrics under the CloudWatch namespace
        // taken from the `service.namespace` resource attribute. Without a
        // Resource the endpoint accepts the POST (HTTP 200) but has no namespace
        // to file under and silently drops the metrics. `Resource::default()`
        // runs env detection (OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME); if
        // the user set no service.namespace, fall back to a default so metrics
        // always land somewhere queryable.
        let has_ns = std::env::var("OTEL_RESOURCE_ATTRIBUTES")
            .map(|v| v.contains("service.namespace"))
            .unwrap_or(false);
        let resource = if has_ns {
            opentelemetry_sdk::Resource::default()
        } else {
            opentelemetry_sdk::Resource::default().merge(&opentelemetry_sdk::Resource::new([
                KeyValue::new("service.namespace", "DynamoDBStreamsConsumer"),
            ]))
        };
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();
        let meter: Meter = provider.meter("amazon-dynamodb-streams-consumer");

        let lag = meter
            .i64_gauge("ddbstreams.consumer.millis_behind_latest")
            .with_description("Consumer lag: now - newest record ApproximateCreationDateTime")
            .with_unit("ms")
            .build();
        let records = meter
            .u64_counter("ddbstreams.consumer.records_processed")
            .with_description("Records delivered to the record processor")
            .build();
        let bytes = meter
            .u64_counter("ddbstreams.consumer.bytes_processed")
            .with_description("Payload bytes delivered")
            .with_unit("By")
            .build();
        let describes = meter
            .u64_counter("ddbstreams.consumer.describe_stream.count")
            .with_description("DescribeStream calls issued by the shard-sync leader")
            .build();
        let shard_ends = meter
            .u64_counter("ddbstreams.consumer.shard_end.count")
            .with_description("Shards that reached SHARD_END")
            .build();
        let lease_acquired = meter
            .u64_counter("ddbstreams.consumer.lease.acquired")
            .with_description("Shard leases this worker acquired or created")
            .build();
        let lease_lost = meter
            .u64_counter("ddbstreams.consumer.lease.lost")
            .with_description("Shard leases this worker lost (stolen or expired)")
            .build();
        let leases_held = meter
            .u64_gauge("ddbstreams.consumer.lease.held")
            .with_description("Shard leases this worker currently holds")
            .build();
        let slot_wait = meter
            .u64_gauge("ddbstreams.consumer.processing.slot_wait_ms")
            .with_description(
                "Time a batch waited to acquire a processing slot (max_processing_concurrency)",
            )
            .with_unit("ms")
            .build();
        let max_concurrency = meter
            .u64_gauge("ddbstreams.consumer.processing.max_concurrency")
            .with_description("Configured max_processing_concurrency cap (0 = unbounded)")
            .build();

        // Base dimensions on every metric: worker id + stream, from the same env
        // the sidecar already uses. Empty values are omitted.
        let mut base_attrs = Vec::new();
        if let Ok(owner) = std::env::var("DDB_STREAMS_CONSUMER_OWNER") {
            if !owner.is_empty() {
                base_attrs.push(KeyValue::new("worker", owner));
            }
        }
        if let Ok(stream) = std::env::var("DDB_STREAMS_CONSUMER_STREAM_ARN") {
            if !stream.is_empty() {
                base_attrs.push(KeyValue::new("stream", stream));
            }
        }

        Ok(Self {
            _provider: provider,
            lag,
            records,
            bytes,
            describes,
            shard_ends,
            lease_acquired,
            lease_lost,
            leases_held,
            slot_wait,
            max_concurrency,
            base_attrs,
        })
    }

    /// Force an immediate export (used on shutdown and by the live test).
    pub fn force_flush(&self) -> Result<(), BoxError> {
        self._provider.force_flush().map_err(|e| e.into())
    }
}

impl OtelMetricsSink {
    /// Build the attribute set for a shard-scoped metric: base dimensions
    /// (worker/stream) plus `shard_id`.
    fn shard_attrs(&self, shard_id: &str) -> Vec<KeyValue> {
        let mut a = self.base_attrs.clone();
        a.push(KeyValue::new("shard_id", shard_id.to_string()));
        a
    }
}

impl MetricsSink for OtelMetricsSink {
    fn on_batch(&self, m: &ShardMetrics<'_>) {
        let attrs = self.shard_attrs(m.shard_id);
        self.records.add(m.records, &attrs);
        self.bytes.add(m.bytes, &attrs);
        if let Some(lag) = m.millis_behind_latest {
            self.lag.record(lag, &attrs);
        }
    }
    fn on_shard_end(&self, shard_id: &str) {
        self.shard_ends.add(1, &self.shard_attrs(shard_id));
    }
    fn on_describe_stream(&self) {
        self.describes.add(1, &self.base_attrs);
    }
    fn on_lease_acquired(&self, shard_id: &str) {
        self.lease_acquired.add(1, &self.shard_attrs(shard_id));
    }
    fn on_lease_lost(&self, shard_id: &str) {
        self.lease_lost.add(1, &self.shard_attrs(shard_id));
    }
    fn on_leases_held(&self, count: u64) {
        self.leases_held.record(count, &self.base_attrs);
    }
    fn on_processing_slot_wait(&self, shard_id: &str, wait_ms: u64) {
        self.slot_wait.record(wait_ms, &self.shard_attrs(shard_id));
    }
    fn on_max_processing_concurrency(&self, cap: u64) {
        self.max_concurrency.record(cap, &self.base_attrs);
    }
}

/// A SigV4-signing OTLP/HTTP transport. Implements `opentelemetry_http::HttpClient`
/// so the OTLP exporter posts through it; when `sign` is set, each request is
/// AWS SigV4-signed (service `monitoring`) using the ambient credentials, which
/// is what the CloudWatch OTLP endpoint requires.
mod sigv4 {
    use super::BoxError;
    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
    use aws_sigv4::sign::v4;
    use aws_smithy_runtime_api::client::identity::Identity;
    use bytes::Bytes;
    use http::{Request, Response};
    use std::time::SystemTime;

    #[derive(Debug)]
    pub struct SigV4HttpClient {
        pub inner: reqwest::Client,
        pub creds: Option<SharedCredentialsProvider>,
        pub region: String,
        pub service: String,
        pub sign: bool,
    }

    impl SigV4HttpClient {
        async fn sign_headers(
            &self,
            method: &str,
            uri: &str,
            headers: &[(String, String)],
            body: &[u8],
        ) -> Result<Vec<(String, String)>, BoxError> {
            let provider = self
                .creds
                .as_ref()
                .ok_or("no AWS credentials provider for SigV4")?;
            let creds =
                aws_credential_types::provider::ProvideCredentials::provide_credentials(provider)
                    .await?;
            let identity: Identity = creds.into();
            let settings = SigningSettings::default();
            let params = v4::SigningParams::builder()
                .identity(&identity)
                .region(&self.region)
                .name(&self.service)
                .time(SystemTime::now())
                .settings(settings)
                .build()?;
            let header_refs: Vec<(&str, &str)> = headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let signable = SignableRequest::new(
                method,
                uri,
                header_refs.into_iter(),
                SignableBody::Bytes(body),
            )?;
            let (instructions, _sig) = sign(signable, &params.into())?.into_parts();
            // Collect the header additions SigV4 wants applied.
            let mut out: Vec<(String, String)> = headers.to_vec();
            for (name, value) in instructions.headers() {
                out.push((name.to_string(), value.to_string()));
            }
            Ok(out)
        }
    }

    #[async_trait::async_trait]
    impl opentelemetry_http::HttpClient for SigV4HttpClient {
        async fn send(
            &self,
            request: Request<Vec<u8>>,
        ) -> Result<Response<Bytes>, opentelemetry_http::HttpError> {
            let method = request.method().clone();
            let uri = request.uri().to_string();
            let body = request.body().clone();
            let mut headers: Vec<(String, String)> = request
                .headers()
                .iter()
                .filter_map(|(k, v)| {
                    v.to_str()
                        .ok()
                        .map(|v| (k.as_str().to_string(), v.to_string()))
                })
                .collect();

            if self.sign {
                headers = self
                    .sign_headers(method.as_str(), &uri, &headers, &body)
                    .await
                    .map_err(|e| -> opentelemetry_http::HttpError { e.to_string().into() })?;
            }

            let mut req = self.inner.request(method, &uri);
            for (k, v) in &headers {
                req = req.header(k, v);
            }
            let resp = req.body(body).send().await?;
            let status = resp.status();
            let bytes = resp.bytes().await?;
            Response::builder()
                .status(status)
                .body(bytes)
                .map_err(|e| -> opentelemetry_http::HttpError { Box::new(e) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amazon_dynamodb_streams_consumer_core::metrics::ShardMetrics;

    /// Live: send a metric to CloudWatch's native OTLP endpoint (SigV4-signed)
    /// and assert the export is accepted. Env-gated; needs AWS creds + region.
    ///   DDB_STREAMS_OTLP_CW_IT=1 AWS_REGION=us-east-1 \
    ///     OTEL_EXPORTER_OTLP_ENDPOINT=https://monitoring.us-east-1.amazonaws.com \
    ///     cargo test -p amazon-dynamodb-streams-consumer-sidecar --features otel otlp_cloudwatch -- --nocapture
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn otlp_cloudwatch_live_export() {
        if std::env::var("DDB_STREAMS_OTLP_CW_IT").is_err() {
            eprintln!("skipping live OTLP→CloudWatch test (set DDB_STREAMS_OTLP_CW_IT=1)");
            return;
        }
        let sink = OtelMetricsSink::from_env().await.expect("build OTLP sink");
        sink.on_batch(&ShardMetrics {
            shard_id: "otlp-live-test",
            records: 1,
            bytes: 64,
            millis_behind_latest: Some(123),
        });
        sink.on_describe_stream();
        // Force an immediate SigV4-signed OTLP/HTTP export to CloudWatch.
        sink.force_flush()
            .expect("force_flush export accepted by CloudWatch");
        eprintln!("[otlp] export accepted by CloudWatch OTLP endpoint");
    }
}
