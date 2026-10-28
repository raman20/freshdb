#!/usr/bin/env bash
# Download the pinned tla2tools.jar to ${1:-$HOME/tla/tla2tools.jar} and
# verify its checksum. Used by CI and for local runs, so both use the same
# jar. v1.7.4 is the latest stable TLA+ release (TLC 2.19, 2024-08-08);
# its asset is immutable. Do not pin v1.8.0: it is a prerelease tag whose
# jar is rebuilt from master every night, so its checksum changes daily.
set -euo pipefail
VER=v1.7.4
SUM=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
out=${1:-$HOME/tla/tla2tools.jar}
mkdir -p "$(dirname "$out")"
if [ ! -f "$out" ] || ! echo "$SUM  $out" | sha256sum -c --status; then
  curl -fsSL -o "$out.tmp" "https://github.com/tlaplus/tlaplus/releases/download/$VER/tla2tools.jar"
  mv "$out.tmp" "$out"
fi
echo "$SUM  $out" | sha256sum -c
