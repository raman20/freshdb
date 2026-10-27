#!/usr/bin/env python3
"""Before/after tables for docs/benchmarks.md "Changes since v0.1.0".

Reads only result JSON under bench/results; nothing is typed by hand.
Usage: python3 bench/scripts/changes.py  (from the repo root)
"""
import json, os, sys

R = os.path.join(os.path.dirname(__file__), "..", "results")
BASE = "2026-10-27"
NEW = "2026-10-27-after-journal"
VEC = "2026-10-27-after-vector"


def load(d, f):
    p = os.path.join(R, d, f)
    return json.load(open(p)) if os.path.exists(p) else None


def ok(r):  # successful closed-loop ops/s, as in the v0.1.0 tables
    c = r["closed_loop"]
    return (c["ops"] - c["errors"]) / c["seconds"]


def ycsb_cells(d):
    return {(r["workload"], r["clients"]): r for r in d["runs"]} if d else {}


def gib(s):
    return "%.2f GiB" % (int(s.split()[0]) / 1024 / 1024)


def main():
    out = []
    b = load(BASE, "ycsb-freshdb-fsync-true.json")
    n = load(NEW, "ycsb-freshdb-fsync-true.json")
    j = load(NEW, "ycsb-freshdb-journal-fsync-true.json")
    pb = ycsb_cells(load(BASE, "ycsb-pebble-fsync-true.json"))
    B, N, J = ycsb_cells(b), ycsb_cells(n), ycsb_cells(j)
    out.append("| | v0.1.0 | now (FS store) | now (journal store) | Pebble (v0.1.0 run) |")
    out.append("|---|---|---|---|---|")
    out.append("| peak RSS, whole YCSB run | %s | %s | %s | %s |" % (
        gib(b["params"]["peak_rss"]), gib(n["params"]["peak_rss"]),
        gib(j["params"]["peak_rss"]) if j else "-",
        gib(load(BASE, "ycsb-pebble-fsync-true.json")["params"]["peak_rss"])))
    out.append("")
    out.append("YCSB, fsync on, successful closed-loop ops/s (F: + RMWs that gave up after 16 retries):")
    out.append("")
    out.append("| cell | v0.1.0 | now (FS) | now (journal) | Pebble (v0.1.0 run) |")
    out.append("|---|---|---|---|---|")
    for w in "ABCFD":
        for c in (1, 8, 64):
            def f(m):
                r = m.get((w, c))
                if not r:
                    return "-"
                s = "%.0f" % ok(r)
                if w == "F":
                    s += " (%d gave up)" % r["closed_loop"]["errors"]
                return s
            out.append("| %s c=%d | %s | %s | %s | %s |" % (w, c, f(B), f(N), f(J), f(pb)))
    out.append("")
    out.append("Streaming, fsync on, 8 producers, end-to-end latency (ms):")
    out.append("")
    out.append("| cell | v0.1.0 p50 / p99 | now FS p50 / p99 | now journal p50 / p99 | NATS (v0.1.0 run) p50 / p99 |")
    out.append("|---|---|---|---|---|")
    ms = {k: load(d, f) for k, d, f in [
        ("b", BASE, "msg-freshdb-fsync-true.json"), ("n", NEW, "msg-freshdb-fsync-true.json"),
        ("j", NEW, "msg-freshdb-journal-fsync-true.json"), ("nats", BASE, "msg-nats-fsync-true.json")]}

    def mcell(d, kind, rate, groups):
        if not d:
            return "-"
        for r in d["runs"]:
            if r["kind"] == kind and r.get("target_rate", 0) == rate and r.get("groups", 1) == groups:
                if kind == "max-throughput":
                    return "%.0f msgs/s" % r["publish_per_sec"]
                e = r["end_to_end_latency"]
                return "%.1f / %.1f" % (e["p50_us"] / 1000, e["p99_us"] / 1000)
        return "-"
    for kind, rate, g in [("fixed-rate", 1000, 1), ("fixed-rate", 5000, 1), ("fixed-rate", 10000, 1),
                          ("fixed-rate", 5000, 4), ("max-throughput", 0, 1)]:
        lab = "max publish (64 producers)" if kind == "max-throughput" else "%dk/s, %d group(s)" % (rate // 1000, g)
        out.append("| %s | %s | %s | %s | %s |" % (lab, *(mcell(ms[k], kind, rate, g) for k in ("b", "n", "j", "nats"))))
    out.append("")
    out.append("Vectors, 128-d cosine, exact search (QPS; recall@10):")
    out.append("")
    out.append("| cell | v0.1.0 freshDB | v0.1.0 flat loop | now freshDB | now flat loop |")
    out.append("|---|---|---|---|---|")
    vb, vn = load(BASE, "vector.json"), load(VEC, "vector.json")

    def v(d, sys_, nn, c):
        for r in d["runs"]:
            if r["system"] == sys_ and r["n"] == nn and r["clients"] == c:
                return "%.0f; %.2f" % (r["qps"], r["recall_at_10"])
        return "-"
    flat = sorted({r["system"] for r in vb["runs"]} - {"freshdb"})[0]
    for nn in (10000, 100000):
        for c in (1, 8):
            out.append("| N=%dk c=%d | %s | %s | %s | %s |" % (nn // 1000, c, v(vb, "freshdb", nn, c), v(vb, flat, nn, c),
                                                             v(vn, "freshdb", nn, c), v(vn, flat, nn, c)))
    out.append("")
    out.append("OCC transfers, fsync on (commits/s; ops that gave up after 16 retries):")
    out.append("")
    out.append("| cell | v0.1.0 freshDB | now freshDB (FS) | Badger (v0.1.0 run) |")
    out.append("|---|---|---|---|")
    tb, tn = load(BASE, "txn-fsync-true.json"), load(NEW, "txn-fsync-true.json")

    def t(d, sys_, c, h):
        for r in (d or {}).get("runs", []):
            if r.get("system") == sys_ and r.get("clients") == c and r.get("hot_keys") == h:
                return "%.0f; %d" % (r["commits_per_sec"], r["gave_up_after_16_retries"])
        return "-"
    for c in (8, 64):
        for h in (1, 16, 256):
            out.append("| c=%d hot=%d | %s | %s | %s |" % (c, h, t(tb, "freshdb", c, h), t(tn, "freshdb", c, h), t(tb, "badger", c, h)))
    print("\n".join(out))


if __name__ == "__main__":
    sys.exit(main())
