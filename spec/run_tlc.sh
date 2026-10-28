#!/usr/bin/env bash
# Model-check specs (default: all). Needs Java 11+ and tla2tools.jar.
#   JAVA=java TLA_JAR=/path/tla2tools.jar spec/run_tlc.sh [FreshLog FreshTxn FreshBranch FreshShred FreshMigrate FreshCompact]
# Exits non-zero if TLC reports any violation or error.
set -euo pipefail
cd "$(dirname "$0")"
JAVA=${JAVA:-java}
TLA_JAR=${TLA_JAR:-tla2tools.jar}
XMX=${TLC_XMX:-6g}
specs=("$@")
[ ${#specs[@]} -eq 0 ] && specs=(FreshLog FreshTxn FreshBranch FreshShred FreshMigrate FreshCompact)
for m in "${specs[@]}"; do
  echo "== $m"
  start=$(date +%s)
  "$JAVA" -XX:+UseParallelGC -Xmx"$XMX" -cp "$TLA_JAR" tlc2.TLC -workers auto -deadlock \
    -metadir "${TMPDIR:-/tmp}/tlc-$m" -config "$m.cfg" "$m.tla" > "${TMPDIR:-/tmp}/tlc-$m.log" 2>&1 || true
  grep -E "violated|Error|states generated|depth|No error" "${TMPDIR:-/tmp}/tlc-$m.log" || true
  echo "($(( $(date +%s) - start ))s)"
  grep -q "Model checking completed. No error has been found." "${TMPDIR:-/tmp}/tlc-$m.log" || { echo "FAIL: $m"; tail -n 40 "${TMPDIR:-/tmp}/tlc-$m.log"; exit 1; }
done
