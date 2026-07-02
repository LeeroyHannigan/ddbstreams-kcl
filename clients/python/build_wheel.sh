#!/usr/bin/env bash
# Build a platform wheel with the sidecar binary bundled inside.
#
#   ./build_wheel.sh [cargo-target-triple]
#
# With no argument it builds for the host platform. In CI, pass a target triple
# (e.g. aarch64-unknown-linux-gnu) to cross-build per-platform wheels.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
bin_name="amazon-dynamodb-streams-consumer-sidecar"
target="${1:-}"

if [ -n "$target" ]; then
  (cd "$root" && cargo build --release -p "$bin_name" --target "$target")
  built="$root/target/$target/release/$bin_name"
else
  (cd "$root" && cargo build --release -p "$bin_name")
  built="$root/target/release/$bin_name"
fi

dest="$here/src/dynamodb_streams_consumer/_bin"
mkdir -p "$dest"
cp "$built" "$dest/$bin_name"
chmod +x "$dest/$bin_name"
echo "bundled: $dest/$bin_name"

python -m build --wheel "$here"
echo "wheel(s):"
ls -1 "$here/dist/"*.whl
