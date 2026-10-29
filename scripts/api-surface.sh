#!/usr/bin/env bash
# Write the exported Go API of the stable-track packages to api/*.txt.
# CI regenerates these and fails on a diff, so every API change shows up
# in review (docs/api-stability.md). Usage: scripts/api-surface.sh
set -euo pipefail
cd "$(dirname "$0")/.."
for p in . store server format lens; do
  name=$( [ "$p" = . ] && echo freshdb || echo "$p" )
  go doc -all "./$p" | grep -E '^(func|type|const|var) |^	[A-Z][A-Za-z0-9_]* +[^ ]' | sed 's/[[:space:]]*\/\/.*$//; s/[[:space:]]*$//' > "api/$name.txt"
done
wc -l api/*.txt
