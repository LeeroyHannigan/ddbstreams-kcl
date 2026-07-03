# Live soak test

A multi-hour correctness soak that drives the **installed** consumer (the
sidecar via a language binding) against **real DynamoDB Streams**. It is not run
on every PR — it needs AWS and hours — but it is the end-to-end proof of the
engine that all bindings share.

## What it verifies (continuously, over N hours)

- **Completeness** — every written `(pk, sk)` is eventually observed.
- **Ordering** — per partition key, first-observation of `sk` is strictly
  increasing (holds within a shard *and* across a real shard roll, since a
  key's child shard is consumed only after its parent completes).
- **Duplicates** — counted; at-least-once redelivery on restart/steal is
  tolerated but must be rare and must never break ordering.
- **Checkpoint resume** — a mid-run graceful stop + restart resumes from the
  checkpoint with no gap.
- **Multi-worker ownership** — a second worker joins for a window; invariants
  must still hold under lease contention.

`soak.py` writes `out/report.json` with a PASS/FAIL verdict and tears down its
tables on pass. Config via env: `SOAK_HOURS` (default 6), `WRITE_RATE` (4/s),
`NUM_KEYS` (8), `RESTART_AT`, `SECOND_AT`, `SECOND_FOR`, `OUTDIR`.

The consumer under test is `boto3`-independent — boto3 is used only by the
harness as the writer/verifier. The soak needs a wheel for the host platform
(the manylinux_2_28 wheel requires glibc >= 2.28: AL2023, Ubuntu 20.04+, etc.).

## Recommended: run on EC2 (auto-refreshing credentials)

An EC2 **instance profile** gives the consumer and boto3 credentials via IMDS
that refresh automatically, so a multi-hour run never hits credential expiry.
`ec2/` holds the least-privilege pieces:

- `trust-policy.json` — EC2 assume-role trust.
- `permissions.json` — DynamoDB control/data + Streams read, scoped to
  `arn:aws:dynamodb:*:*:table/adsc-soak-*` (+ streams). Replace the account id.
- `bootstrap.sh` — runs on the instance (via SSM): installs the consumer from
  the package index + boto3 and starts `soak.py` as a systemd service.

Provision an IAM role + instance profile from those two policies (plus
`AmazonSSMManagedInstanceCore`), launch an AL2023 instance with the profile, and
run `bootstrap.sh` over SSM. Retrieve `out/report.json` (SSM/S3) when done.

## Local / container run

`run_soak.sh` runs the soak detached in a `python:3.12-slim` container, pulling
the consumer from the configured index and injecting credentials as env. Note:
static env creds do not refresh, so cap `SOAK_HOURS` under the credential
lifetime — the EC2 path is preferred for long runs.
