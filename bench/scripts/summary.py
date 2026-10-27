import json, sys, os
D = sys.argv[1]
SYS = ["freshdb", "badger", "pebble", "bbolt", "sqlite"]
def load(f):
    p = os.path.join(D, f)
    return json.load(open(p)) if os.path.exists(p) else None
def fmt(x):
    if x is None: return "-"
    if x >= 1e6: return f"{x/1e6:.2f}M"
    if x >= 1e4: return f"{x/1e3:.0f}k"
    if x >= 1e3: return f"{x/1e3:.1f}k"
    return f"{x:.0f}"
def ms(us):
    if us >= 1000: return f"{us/1000:.0f}" if us >= 10000 else f"{us/1000:.1f}"
    return f"{us/1000:.2f}" if us >= 10 else f"{us/1000:.3f}"
def ycsb(fsync, clients, metric):
    data = {s: load(f"ycsb-{s}-fsync-{fsync}.json") for s in SYS}
    out = ["| workload | clients | " + " | ".join(SYS) + " |", "|---|---|" + "---|" * len(SYS)]
    for wl in "ABCFD":
        for c in clients:
            cells = []
            best = None
            vals = {}
            for s in SYS:
                d = data[s]
                r = next((r for r in d["runs"] if r["workload"] == wl and r["clients"] == c), None) if d else None
                if r is None: cells.append("-"); continue
                if metric == "closed":
                    cl = r["closed_loop"]; e = cl["errors"]
                    v = (cl["ops"] - e) / cl["seconds"]; vals[s] = v
                    cells.append(fmt(v) + (f" (+{e} failed)" if e else ""))
                else:
                    o = r["open_loop"]; l = o["latency"]
                    ni = o.get("not_issued_overload", 0)
                    c2 = f"{ms(l['p50_us'])} / {ms(l['p99_us'])}"
                    if ni: c2 += f" ({ni} not issued)"
                    if o["errors"]: c2 += f" ({o['errors']} err)"
                    cells.append(c2)
            if metric == "closed" and vals:
                b = max(vals, key=vals.get); i = SYS.index(b); cells[i] = "**" + cells[i] + "**"
            out.append(f"| {wl} | {c} | " + " | ".join(cells) + " |")
    return "\n".join(out)
def res(fsync):
    out = ["| | " + " | ".join(SYS) + " |", "|---|" + "---|" * len(SYS)]
    rows = {"load (keys/s)": [], "peak RSS": [], "disk after run": []}
    for s in SYS:
        d = load(f"ycsb-{s}-fsync-{fsync}.json"); p = d["params"] if d else {}
        rows["load (keys/s)"].append(fmt(p.get("load_keys_per_sec")) if d else "-")
        rss = p.get("peak_rss", "-")
        try: rss = f"{int(str(rss).split()[0])/1048576:.1f} GiB"
        except: pass
        rows["peak RSS"].append(rss); rows["disk after run"].append(str(p.get("disk_after_run", "-")))
    for k, v in rows.items(): out.append(f"| {k} | " + " | ".join(v) + " |")
    return "\n".join(out)
which = sys.argv[2]
if which == "closed": print(ycsb(sys.argv[3], [int(x) for x in sys.argv[4].split(",")], "closed"))
elif which == "open": print(ycsb(sys.argv[3], [int(x) for x in sys.argv[4].split(",")], "open"))
elif which == "res": print(res(sys.argv[3]))
