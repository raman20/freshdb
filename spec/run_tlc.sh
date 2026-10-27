#!/usr/bin/env bash
# Model-check every spec. Needs Java 11+ and tla2tools.jar (TLA_JAR).
set -euo pipefail
cd "$(dirname "$0")"
JAVA=${JAVA:-java}
TLA_JAR=${TLA_JAR:-tla2tools.jar}
for m in FreshLog FreshTxn FreshBranch FreshShred; do
  echo "== $m"
  "$JAVA" -XX:+UseParallelGC -Xmx6g -cp "$TLA_JAR" tlc2.TLC -workers auto -deadlock \
    -metadir "/tmp/tlc-$m" -config "$m.cfg" "$m.tla" | grep -E "violated|Error|states generated|depth|No error"
done
