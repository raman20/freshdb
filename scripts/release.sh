#!/usr/bin/env bash
# Build release archives into dist/ (gitignored) for the given version.
# Usage: scripts/release.sh v0.1.0
set -euo pipefail
V=${1:?usage: scripts/release.sh vX.Y.Z}
cd "$(dirname "$0")/.."
rm -rf dist && mkdir -p dist
for t in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64; do
  os=${t%/*} arch=${t#*/}
  name=freshdb_${V}_${os}_${arch}
  ext=""; [ "$os" = windows ] && ext=.exe
  mkdir -p dist/$name
  CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags "-s -w -X main.version=$V" -o dist/$name/freshdb$ext ./cmd/freshdb
  cp LICENSE CHANGELOG.md dist/$name/
  # README links are repo-relative; point them at the tagged tree on GitHub
  python3 - "$V" dist/$name/README.md <<'PY'
import re, sys
v, out = sys.argv[1], sys.argv[2]
base = f"https://github.com/raman20/freshdb/blob/{v}/"
s = open("README.md", encoding="utf-8").read()
fix = lambda m: m.group(1) + (m.group(2) if re.match(r"(https?:|mailto:|#)", m.group(2)) else base + m.group(2))
s = re.sub(r'(\]\()([^)\s]+)', fix, s)
s = re.sub(r'((?:src|href)=")([^"]+)', fix, s)
open(out, "w", encoding="utf-8").write(s)
PY
  if [ "$os" = windows ]; then
    (cd dist && python3 -c "import shutil,sys; shutil.make_archive(sys.argv[1], 'zip', '.', sys.argv[1])" $name)
  else
    tar -C dist -czf dist/$name.tar.gz $name
  fi
done
python3 scripts/sbom.py "$V" > dist/sbom.spdx.json
(cd dist && sha256sum *.tar.gz *.zip sbom.spdx.json > checksums.txt && cat checksums.txt)
# Optional signature over checksums.txt with an SSH key (no extra tools):
#   verify: ssh-keygen -Y verify -f allowed_signers -I release@freshdb \
#             -n freshdb-release -s checksums.txt.sig < checksums.txt
if [ -n "${FRESHDB_SIGN_KEY:-}" ]; then
  ssh-keygen -Y sign -q -f "$FRESHDB_SIGN_KEY" -n freshdb-release dist/checksums.txt
  echo "signed: dist/checksums.txt.sig"
else
  echo "unsigned (set FRESHDB_SIGN_KEY to sign checksums.txt)"
fi
