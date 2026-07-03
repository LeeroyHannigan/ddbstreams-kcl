#!/usr/bin/env bash
# Launch the cleanroom soak detached in a modern-glibc container.
# The consumer is installed FRESH from TestPyPI inside the container; only boto3
# is added for the test harness. Credentials are injected as env at launch.
#
# Usage: bash run_soak.sh            # uses SOAK_HOURS default (6h)
#        SOAK_HOURS=2 bash run_soak.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/out"
mkdir -p "$OUT"

: "${SOAK_HOURS:=6.0}"
: "${WRITE_RATE:=4.0}"
: "${NUM_KEYS:=8}"
REGION="${AWS_REGION:-us-east-1}"

echo "== exporting sandbox credentials (must be freshly refreshed) =="
CREDS_ENV="$(env -u PYTHONPATH -u PYTHONHOME aws configure export-credentials --format env-no-export 2>/dev/null || true)"
if [ -z "$CREDS_ENV" ]; then
  echo "ERROR: could not export credentials. Run 'ada credentials update ...' first." >&2
  exit 1
fi
# shellcheck disable=SC2046
export $(echo "$CREDS_ENV" | sed 's/^/AWS_/;s/^AWS_AWS_/AWS_/' | xargs) 2>/dev/null || true
# export-credentials env-no-export already emits AWS_ACCESS_KEY_ID etc.
eval "$(env -u PYTHONPATH -u PYTHONHOME aws configure export-credentials --format env 2>/dev/null)"

echo "== identity check =="
env -u PYTHONPATH -u PYTHONHOME aws sts get-caller-identity --query Arn --output text

CID=$(docker run -d --name "ddbsc-soak-$(date +%s)" \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
  -e AWS_REGION="$REGION" \
  -e SOAK_HOURS="$SOAK_HOURS" -e WRITE_RATE="$WRITE_RATE" -e NUM_KEYS="$NUM_KEYS" \
  -e OUTDIR=/soak/out \
  -v "$HERE":/soak \
  -w /soak \
  python:3.12-slim \
  bash -c '
    set -e
    pip install --quiet --upgrade pip
    pip install --quiet boto3
    pip install --quiet -i https://test.pypi.org/simple/ amazon-dynamodb-streams-consumer
    python -c "import dynamodb_streams_consumer as m; print(\"consumer\", m.__version__)"
    exec python -u soak.py
  ')

echo "$CID" > "$HERE/container.id"
echo "== soak launched, container: $CID =="
echo "logs:   docker logs -f $CID"
echo "report: $OUT/report.json (written on completion)"
