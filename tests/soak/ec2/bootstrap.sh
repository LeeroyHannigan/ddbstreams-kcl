#!/usr/bin/env bash
# Runs ON the EC2 instance (delivered via SSM). Assumes /var/soak/soak.py exists.
# Uses the instance profile (IMDS) for auto-refreshing credentials, so the soak
# survives arbitrarily long runs with no static-credential expiry.
set -euxo pipefail

: "${SOAK_HOURS:=6.0}"
: "${WRITE_RATE:=4.0}"
: "${NUM_KEYS:=8}"

dnf install -y python3 python3-pip >/dev/null 2>&1 || yum install -y python3 python3-pip

mkdir -p /var/soak/out
python3 -m venv /var/soak/venv
/var/soak/venv/bin/pip install --quiet --upgrade pip
/var/soak/venv/bin/pip install --quiet boto3
# The CONSUMER under test: fresh from TestPyPI (manylinux_2_28 wheel, glibc 2.34 OK on AL2023)
/var/soak/venv/bin/pip install --quiet -i https://test.pypi.org/simple/ amazon-dynamodb-streams-consumer
/var/soak/venv/bin/python -c "import dynamodb_streams_consumer as m; print('consumer', m.__version__)"

cat >/etc/systemd/system/soak.service <<UNIT
[Unit]
Description=amazon-dynamodb-streams-consumer cleanroom soak
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/var/soak
Environment=AWS_REGION=us-east-1
Environment=SOAK_HOURS=${SOAK_HOURS}
Environment=WRITE_RATE=${WRITE_RATE}
Environment=NUM_KEYS=${NUM_KEYS}
Environment=OUTDIR=/var/soak/out
ExecStart=/var/soak/venv/bin/python -u /var/soak/soak.py
Restart=no

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now soak.service
echo "soak.service started; report -> /var/soak/out/report.json"
