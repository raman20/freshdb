#!/usr/bin/env python3
"""Write an SPDX 2.3 JSON SBOM for the freshdb binary to stdout.

Lists the Go modules linked into ./cmd/freshdb (go list -deps) with their
versions and go.sum hashes. Usage: scripts/sbom.py vX.Y.Z > dist/sbom.spdx.json
"""
import datetime, json, subprocess, sys, uuid

version = sys.argv[1] if len(sys.argv) > 1 else "dev"
out = subprocess.run(["go", "list", "-deps", "-f",
                      "{{with .Module}}{{.Path}}|{{.Version}}|{{.Main}}{{end}}",
                      "./cmd/freshdb"], check=True, capture_output=True, text=True).stdout
mods = sorted({tuple(l.split("|")) for l in out.splitlines() if l.strip()})
sums = {}
for line in open("go.sum"):
    p, v, h = line.split()
    if not v.endswith("/go.mod"):
        sums[(p, v)] = h
gover = subprocess.run(["go", "env", "GOVERSION"], capture_output=True, text=True).stdout.strip()


def ref(p):
    return "SPDXRef-" + "".join(c if c.isalnum() else "-" for c in p)


pkgs, rels = [], []
for p, v, main in mods:
    v = version if main == "true" else v
    pk = {"SPDXID": ref(p), "name": p, "versionInfo": v, "downloadLocation": "NOASSERTION",
          "externalRefs": [{"referenceCategory": "PACKAGE-MANAGER", "referenceType": "purl",
                            "referenceLocator": f"pkg:golang/{p}@{v}"}]}
    if (p, v) in sums:
        pk["comment"] = "go.sum " + sums[(p, v)]
    pkgs.append(pk)
    if main != "true":
        rels.append({"spdxElementId": ref("github.com/raman20/freshdb"),
                     "relationshipType": "DEPENDS_ON", "relatedSpdxElement": ref(p)})
pkgs.append({"SPDXID": "SPDXRef-go", "name": "go", "versionInfo": gover.removeprefix("go"),
             "downloadLocation": "https://go.dev/dl/"})
rels.append({"spdxElementId": "SPDXRef-DOCUMENT", "relationshipType": "DESCRIBES",
             "relatedSpdxElement": ref("github.com/raman20/freshdb")})
json.dump({"spdxVersion": "SPDX-2.3", "dataLicense": "CC0-1.0", "SPDXID": "SPDXRef-DOCUMENT",
           "name": f"freshdb-{version}",
           "documentNamespace": f"https://github.com/raman20/freshdb/sbom/{version}/{uuid.uuid4()}",
           "creationInfo": {"created": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "creators": ["Tool: freshdb scripts/sbom.py"]},
           "packages": pkgs, "relationships": rels}, sys.stdout, indent=1)
print()
