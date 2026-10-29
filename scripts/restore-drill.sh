#!/usr/bin/env bash
# Restore drill: a live server takes writes and compactions while an
# online backup runs; the server is then killed (-9), the backup restored
# into a fresh directory, verified, and checked to hold every write
# acknowledged before the backup started (as a gap-free prefix).
# Usage: scripts/restore-drill.sh [FRESHDB_BINARY]   (default: go build)
set -euo pipefail
BIN=${1:-}
W=$(mktemp -d)
trap 'kill -9 $SRV 2>/dev/null || true; rm -rf "$W"' EXIT
if [ -z "$BIN" ]; then BIN=$W/freshdb; go build -o "$BIN" ./cmd/freshdb; fi
PORT=$((20000 + RANDOM % 20000))
export FRESHDB_ADDR=http://127.0.0.1:$PORT FRESHDB_TOKEN=drill-token
"$BIN" serve --db "$W/src" --listen 127.0.0.1:$PORT >"$W/serve.log" 2>&1 & SRV=$!
for _ in $(seq 50); do curl -fs "$FRESHDB_ADDR/readyz" >/dev/null && break; sleep 0.1; done
N=${N:-400}
( for i in $(seq 0 $((N - 1))); do "$BIN" put "k$i" "v$i" >/dev/null && echo $i >"$W/acked"; done ) & WR=$!
( while kill -0 $WR 2>/dev/null; do "$BIN" compact >/dev/null; sleep 0.2; done ) & CP=$!
while [ ! -s "$W/acked" ] || [ "$(cat "$W/acked")" -lt $((N / 2)) ]; do sleep 0.05; done
MIN=$(( $(cat "$W/acked") + 1 ))
"$BIN" backup "$W/backup" >"$W/backup.json"
wait $WR $CP
kill -9 $SRV
unset FRESHDB_ADDR
"$BIN" restore "$W/backup" --db "$W/restored" 2>"$W/restore.log" || { cat "$W/restore.log"; exit 1; }
M=0
while v=$("$BIN" get "k$M" --db "$W/restored" 2>/dev/null); do
  [ "$v" = "v$M" ] || { echo "FAIL: k$M = $v"; exit 1; }
  M=$((M + 1))
done
for j in $(seq $((M + 1)) $((M + 10))); do
  if "$BIN" get "k$j" --db "$W/restored" >/dev/null 2>&1; then echo "FAIL: gap at k$M"; exit 1; fi
done
[ "$M" -ge "$MIN" ] || { echo "FAIL: restored $M keys, $MIN acknowledged before the backup"; exit 1; }
echo "DRILL OK: $MIN writes acknowledged before the backup, $M restored (gap-free prefix), objects: $(grep -o '"objects": *[0-9]*' "$W/backup.json")"
