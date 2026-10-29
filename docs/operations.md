# Operations

How to choose storage, size memory, migrate, back up and run freshDB.
Applies to unreleased `main`; v0.1.0 differences are called out. Everything
here is pre-1.0: do not run production data on it yet.

## Storage URLs

One config value selects the storage: `--db URL` or `FRESHDB_URL` for the
CLI and server, the `url` argument of `freshdb.Open` in Go. Default:
`./freshdb-data`.

| URL | Store | Notes |
|---|---|---|
| `./dir`, `/abs/dir`, `file:///abs/dir` | local, auto-detected layout | existing directory: keeps its layout; new or empty directory: **journal** on Linux/macOS/BSD, **files** on Windows |
| `journal:///abs/dir` | local, journal layout | refuses a directory that holds the files layout |
| `fs:///abs/dir` | local, files layout (one file per object, as in v0.1.0) | refuses a directory that holds `journal.fj` |
| `s3://bucket/prefix?endpoint=host:port&insecure=1&create=1` | any S3-compatible store | `insecure=1` = plain HTTP, `create=1` = create the bucket; credentials from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| `mem://name` | in memory | tests only; gone when the process exits |

Layout detection (`store.DetectLayout`): a directory with `journal.fj` is
journal; any other content (except an empty `.tmp/`) is files; an empty or
missing directory gets the platform default. A directory never holds both.

```console
$ freshdb --db journal:///tmp/qs/m get a         # m was created with fs://
freshdb: open journal:///tmp/qs/m: store: /tmp/qs/m holds the one-file-per-object layout; open it as fs:// or convert it with `freshdb migrate`
```

### Which local layout?

- **journal** (default for new directories on Unix): one preallocated,
  append-only `journal.fj`. A commit is one `write` plus one `fdatasync`.
  The file grows in 8 MiB zero-filled chunks, each followed by an `fsync`
  on the commit path (a ~15–40 ms stall every few MB written; not fixed
  yet). A fresh database is therefore an 8 MiB file. Writers take an
  exclusive `flock`.
- **files** (v0.1.0, Windows): one file per object. Each commit writes a
  temp file, fsyncs it, links/renames it into place and fsyncs the
  directory: two fsyncs per commit, so single-client durable writes are
  about 9× slower (YCSB A, 1 client: 5.2k vs 46k ops/s in
  [benchmarks.md](benchmarks.md)).

The bytes of every object (segments, manifests, keys) are identical in
both; see [formats.md](formats.md).

### Object stores

The store must support create-only conditional writes
(`If-None-Match: *` on PUT); `Open` checks this and refuses otherwise.
**Only MinIO is tested** (in CI). AWS S3 and Cloudflare R2 document the
needed semantics but have not been tested with freshDB. Values are read
back with ranged GETs. Run `freshdb s3check` against a new endpoint first;
see [s3.md](s3.md) for the guarantees, retries and request cost.

## Migrating a v0.1.0 directory

v0.1.0 directories use the files layout and keep working unchanged. To
convert one to the journal (Unix only):

1. Stop every process using it (a server, or anything embedding it). The
   files layout has no lock, so this is not checked.
2. Run:

```console
$ freshdb migrate --db ./m
migrate: copied and verified 5 objects (330 bytes)
freshdb migrate: ./m uses the journal layout
$ ls -d m*
m  m.bak-files
```

`migrate` copies every object into `DIR.migrate-tmp`, verifies every byte,
fsyncs, renames it to `DIR.migrated`, moves the original to
`DIR.bak-files`, and renames the copy into place. If it is interrupted,
run the same command again; it resumes from whichever names exist. The
backup is never deleted automatically; delete it yourself once you are
satisfied. To go back: `freshdb migrate --db ./m --to files` (keeps
`DIR.bak-journal`).

Note: the directory must be given with `--db` (or `FRESHDB_URL`); a
positional argument is ignored. The protocol is model-checked in
[`spec/FreshMigrate.tla`](../spec/FreshMigrate.tla) and described in
[design/01-journal-default.md](design/01-journal-default.md).

## Memory and cache sizing

What lives in RAM (per open database):
- every KV key and its version history (MVCC, 40 bytes per version plus
  the key; kept until `Compact`), with values of up to 64 B inline;
- topic messages, documents and doc indexes, and vectors;
- the **value cache** for KV values over 64 B, which are otherwise read
  from the log (`pread` locally, ranged GET on S3) and CRC-checked.

The value cache is off the Go heap (one anonymous `mmap` arena on Unix), so
RSS grows by about its size once it fills. Go API options:

| Option | Default | Effect |
|---|---|---|
| `WithValueCacheBytes(n)` | 1/16 of the memory limit (cgroup v2 `memory.max`, else MemTotal), 16–256 MiB | cache size; negative disables the cache |
| `WithInlineMax(n)` | 64 B | KV values up to `n` bytes stay in RAM; negative keeps none inline |
| `WithSnapshotEvery(n)` | 0 (off) | KV+topic snapshot every `n` commits; written synchronously and does not speed up open yet |

The CLI and `freshdb serve` use the defaults; they have no flags for these
yet. For scale: the 1M × 1 KiB YCSB run peaked at 1.55 GiB RSS with the
defaults on the journal layout (2.05 GiB before the packed versions; v0.1.0:
7.1 GiB). Lower the cache for small machines, raise it if your hot set is
larger and reads miss.

**Go GC settings.** About half of RSS with defaults is the Go garbage
collector's headroom (`GOGC=100` lets the heap reach ~2× the live heap).
freshDB does not set these itself; set them on the process:
- `GOMEMLIMIT` caps Go memory (not the value-cache arena, which is
  outside the Go heap): RSS ≈ limit + cache. On the YCSB run,
  `GOMEMLIMIT=800MiB` gave 1.06 GiB peak with reads within noise, but
  read-latest (D) at 64 clients fell 927k → 599k ops/s; at 600 MiB with a
  64 MiB cache, 0.69 GiB but D fell to 169k (the GC runs constantly).
  A reasonable start is ~1.5× the live heap after GC.
- Run `serve --compact-every` so the live heap stops growing with history:
  with compaction between workloads and `GOMEMLIMIT=800MiB` the run peaked
  at 1.20 GiB, ended at 0.93 GiB and lost no throughput.
- Raising `GOGC` (e.g. 200) does the opposite: more RSS, less GC CPU (not measured here).

Full breakdown and curve: [design/07-memory.md](design/07-memory.md).

## Running a server

```sh
freshdb serve --db ./data --listen 127.0.0.1:7070    # FRESHDB_LISTEN also works
export FRESHDB_ADDR=http://127.0.0.1:7070            # point CLI calls at it
```

- One writer per database. Opening a database writes a new manifest with a
  higher epoch and **fences** the previous writer (`ErrFenced`, HTTP 503).
  Don't open the same URL from two servers, and don't run embedded CLI
  calls against a URL a server is using; use `--addr` instead.
- Every embedded CLI call is an open (a manifest and a fence segment), so
  prefer a long-running server.
- The server binds to `127.0.0.1` by default. To expose it, set tokens
  (`--token` / `FRESHDB_TOKEN` for one admin token, `--token-file` for
  scoped tokens) and TLS (`--tls-cert`, `--tls-key`); `serve` refuses a
  non-loopback `--listen` without a token unless you pass
  `--insecure-no-auth`. Clients send the token with `--addr ... --token`
  or `FRESHDB_TOKEN`. `GET /healthz` and `/readyz` need no token.
- Token file, one per line (`#` comments; a bare `TOKEN` line is admin):

      read   r-token-123  name=dashboards
      write  w-token-456  name=orders-svc kv:orders/* topic:orders docs:orders
      admin  a-token-789  name=ops

  read: gets, scans, topic reads/streams, searches, log, verify; write:
  adds puts, deletes, tx, publish, branch create; admin: adds rewind,
  promote, branch delete, shred, indexes, ANN, compact, gc. Resources
  (`kv:PREFIX*`, `topic:`, `docs:`, `vectors:`) limit a token to those
  collections and exclude database-wide routes. 401 = unknown token,
  403 = not allowed.
- Rotation without restart: the token file is re-read within 2 s of a
  change, the certificate files within 10 s, both at once on `SIGHUP`
  (`kill -HUP <pid>`). A file that does not parse (or a cert/key pair that
  does not match) is logged and the old one stays in use. Rotate a token
  by adding the new one, moving clients, then removing the old one; open
  SSE streams of a removed token end at their next event or ping (≤ 15 s).
- `--max-streams` (default 1024) and `--max-waiters` (default 4096) cap
  open SSE streams and long-polls (`wait>0`); over the cap: 503 with
  `Retry-After: 1`. `-1` removes a cap.
- Supply the KEK with `--kek-file` or `FRESHDB_KEK` (64 hex chars or
  base64 of 32 bytes) on every process that opens the database, including
  embedded CLI calls. A wrong key fails at open with "wrong key-encryption
  key". A database first opened without a key stays in dev-key mode
  (no rekey yet).
- Consumers can long-poll (`?wait=30s`, max 60 s) or hold an SSE stream
  (`GET /v1/topics/{t}/stream`, a `: ping` comment every 15 s). Each one
  holds a connection and a goroutine (capped by `--max-streams` /
  `--max-waiters`); cap connections at a proxy if clients are untrusted. Proxies must not
  buffer `text/event-stream` responses (nginx: `proxy_buffering off`).
  Streams end when the server shuts down; clients resume with
  `Last-Event-ID`.
- Shutdown: `SIGTERM`/`SIGINT` drains: `/readyz` turns 503 `draining`,
  SSE streams end with `event: shutdown`, long polls return, in-flight
  requests finish (up to `--shutdown-timeout`, default 30 s), then the
  database closes and the log says `closed cleanly`. A second signal
  exits at once; acknowledged writes are durable either way. Point your
  orchestrator's stop timeout above `--shutdown-timeout`.
- `serve` binds the port before opening the database, so a start on a
  busy port fails without fencing the running server.
- Timeouts per request: `--read-timeout` (default 5m) bounds reading a
  request body (stalled uploads are cut off, nothing written);
  `--write-timeout` (default 2m) bounds writing a response (streams renew
  it per event, long polls get it on top of `wait`, admin routes such as
  compact/gc/backup are exempt); idle keep-alive connections close after
  2m. `-1` disables a timeout.
- Limits: keys and topic/collection/subject names ≤ 8 KiB
  (`MaxKeyBytes`), a transaction ≤ 64 MiB encoded and 1M ops, a request
  body ≤ `--max-body` (96 MiB); over a limit: 413.
- Who holds the write lease: every CLI process records itself in
  `lease/<epoch>.json` (version, command, pid, host, start). A takeover
  logs `took the write lease (epoch N) from epoch M (...)`, and the fenced
  server's errors say `now held by epoch N (...)`. Compaction or GC
  started while one runs: 409.
- To scale, shard by database (one per tenant or agent).

## Backups and restore

- **Online backup:** `freshdb backup DEST` (through a running server with
  `--addr`, admin token; or `POST /v1/backup?to=DEST`; or `db.Backup` in
  Go). DEST is any store URL the *server* can write: a path (journal
  layout by default), `fs:///path`, `s3://bucket/prefix?...`. Commits keep
  going; compaction and GC run but their deletes wait until the copy ends.
  The backup is consistent: it opens at the newest manifest when the copy
  started, plus any commits acknowledged during the copy, never a gap.
- **Incremental:** run it again into the same DEST: only new objects are
  copied and objects the database no longer has are removed (so a
  shredded subject's key leaves the backup at the next run). A DEST
  holding anything else (another database's backup, unrelated files) is
  refused.
- **Complete or not:** a backup ends by writing `backup/info.json`
  (objects, bytes, a SHA-256 digest of every object). An interrupted run
  leaves no record, and restore refuses it; re-run the backup.
- **Restore:** `freshdb restore BACKUP --db NEW` copies into an empty
  location, re-reads and checks every object against the digest, opens
  the database and runs `verify`. Pass the same KEK as the source
  (`--kek-file` / `FRESHDB_KEK`); without it a KEK-mode backup cannot be
  opened (keep the KEK separately from backups). In dev-key mode the KEK
  is inside the backup.
- **Point in time:** `freshdb restore BACKUP --db NEW --at LSN [--branch
  main]` rewinds the branch to that log position after the restore (the
  previous head stays in the reflog). Only history the backup still holds
  is reachable: compaction drops history below each head, GC keeps what
  `--keep-for` / `--keep-commits` say. To get further back, keep older
  backups (one DEST per retention slot).
- **Drill:** `scripts/restore-drill.sh` runs a live server with writes and
  compactions, backs it up, kills it with `-9`, restores, and checks that
  every write acknowledged before the backup is there with no gaps. Run it
  against your binary after upgrades.
- **Shred and backups:** a backup taken before a shred still holds that
  subject's DEK and can decrypt its data until that backup is updated or
  deleted. Shredding never waits for a running backup, so a backup that
  overlaps a shred may or may not hold the key. If you rely on shred, keep
  backup retention short (and expire noncurrent S3 versions of `keys/`).
  See [security.md](security.md).
- **Offline copy** still works: stop the writer, copy the directory or
  prefix, `freshdb --db COPY verify`.

## Compaction

New on `main` (not in v0.1.0). Compaction writes a **base image** of every
log at its head: the latest live version of each key, every topic message,
and large values in chunk files. It then commits a manifest that lists the
old segments, bases and snapshots as garbage, and deletes them once a grace
period has passed. Design: [design/03-compaction.md](design/03-compaction.md);
model: [`spec/FreshCompact.tla`](../spec/FreshCompact.tla).

```sh
freshdb compact                       # embedded, against FRESHDB_URL or --db; grace 0s
freshdb serve --compact-every 1h --compact-grace 5m   # background, on a timer
curl -XPOST '127.0.0.1:7070/v1/compact?grace=5m'      # on a running server
```

`compact` prints what it did, for example
`{"base_bytes":300310,"deleted":401,"keys":3,"logs":1,"messages":0,"pending":0}`.
`pending` counts garbage still waiting for its grace period; the next
compaction deletes it.

What you give up:
- **Time travel, `rewind` and `branch create --at` below the base.**
  They return `410` (`ErrCompacted`), e.g.
  `history before the compaction base was compacted away: LSN 1 < base 6`.
- **Structural sharing between branches.** Each branch gets its own base.
- `log` and `verify` start at the base (the base records the hash-chain head,
  so the chain still verifies from there).

What it does not do yet: no retention for topics (every message is kept),
no TTL. Values of a subject
shredded after the last compaction are dropped by the next one.

Disk space: on the files layout and S3, replaced objects are deleted. In the
journal file they are hole-punched on Linux, so `du` drops but the file's
apparent size does not (tested: 25 MiB → 5.7 MiB after overwriting three
large keys 200 times). On macOS/BSD the bytes are zeroed instead and space
is not returned.

Measured (`freshdb-bench compact`, 10k keys × 1 KiB overwritten 50 times:
9.8 MiB live of 488 MiB written; see [benchmarks](benchmarks.md#changes-since-v010-unreleased-main)):
journal 504 → 34 MiB allocated and open 1.6 s → 30 ms; files layout 5,004
files → 9. Commits wait while it runs (0.9 s here). `compact` never deletes
manifests or retired logs; use `gc` for that.

## GC and retention

`gc` is compaction with a history window, plus collection of what nothing
can reach any more. Design: [design/08-gc-retention.md](design/08-gc-retention.md);
model: [`spec/FreshGC.tla`](../spec/FreshGC.tla).

```sh
freshdb gc --keep-for 24h --keep-commits 1000   # keep both windows (the larger wins)
freshdb gc --keep-reflog 64 --keep-manifests 16 # the defaults
freshdb gc --shrink                             # then rewrite the journal file (offline)
freshdb serve --gc-every 1h --keep-for 24h      # background, grace = --compact-grace
curl -XPOST '127.0.0.1:7070/v1/gc?keep-for=24h&grace=5m'
freshdb branch delete NAME                      # GC drops its log once the reflog entry ages out
```

- Each log is compacted at the newest segment boundary that keeps the last
  `--keep-commits` commits and everything newer than `--keep-for`; time
  travel, `rewind` and `branch create --at` work inside the window and
  return `410` below it. A branch's parent keeps what the branch needs.
- Logs no branch and no kept reflog entry can reach are dropped. The
  reflog keeps the newest `--keep-reflog` entries and any newer than
  `--keep-for`.
- Manifests older than the newest `--keep-manifests` are deleted. A writer
  that was fenced and comes back cannot commit into a deleted slot: every
  manifest write re-lists and backs out if a newer one exists.
- `--shrink` copies the live records into a new journal file and renames it
  over the old one. It refuses while any process has the journal open
  (`journal.lock`). **Binaries older than this change do not take that
  lock**: stop them before shrinking. A crash mid-shrink leaves the old file
  intact (the temporary copy is removed on the next open).

Measured (`freshdb-bench gc`, 20 cycles of overwrite + branch + rewind churn,
195 MiB written, 9.8 MiB live): disk 441 MiB with `compact`, 38.5 MiB with
`gc`, 32 MiB with `gc --shrink` after each cycle. Without shrinking, the
journal still keeps about one 4 KiB block per deleted object (dead record
headers and hole edges; +0.33 MiB per cycle here) and its apparent size
only grows, so run `gc --shrink` during maintenance windows.


## Health checks and monitoring

- `GET /healthz`: the process is up (liveness). No token.
- `GET /readyz`: the database can take writes: open, not fenced by another
  writer, no writer halted by a storage error, and the store answers (two
  reads, cached 1 s). 503 with `not ready: closed | fenced | writer halted
  ... | storage unreachable`. No token. Use it for readiness and for
  load-balancer health; a halted or fenced server needs a restart (or to
  be retired), not more traffic.
- `GET /metrics`: Prometheus text format; needs an admin token when auth
  is on (`bearer_token_file` in the scrape config). Families:
  `freshdb_commits_total`, `freshdb_commit_errors_total`,
  `freshdb_conflicts_total`, `freshdb_commit_latency_seconds` (histogram),
  `freshdb_segments_total`, `freshdb_segment_bytes_total`,
  `freshdb_segment_write_seconds`, `freshdb_compactions_total{kind}`,
  `freshdb_compaction_errors_total{kind}`,
  `freshdb_compaction_pause_seconds`, `freshdb_snapshots_total`,
  `freshdb_snapshot_errors_total`, `freshdb_value_cache_*`,
  `freshdb_branches`, `freshdb_head_lsn{branch="main"}`,
  `freshdb_writer_halted`, `freshdb_http_requests_total{route,code}`,
  `freshdb_http_request_duration_seconds{route}` (streams and long-polls
  excluded), `freshdb_http_unauthorized_total`, `freshdb_http_inflight`,
  `freshdb_http_streams`, `freshdb_http_waiters`, `freshdb_build_info`, and
  `go_*` runtime gauges. Embedded users get the same numbers from
  `db.Stats()`, `db.Healthy()` and `db.Ping(ctx)`.
- Suggested alerts: `freshdb_writer_halted == 1` (page: commits fail until
  restart; check disk space / store errors in the log);
  `increase(freshdb_commit_errors_total[5m]) > 0`;
  `histogram_quantile(0.99, rate(freshdb_commit_latency_seconds_bucket[5m]))`
  above your SLO; `increase(freshdb_compaction_errors_total[1h]) > 0`;
  `/readyz` failing.
- Logs: `--log-format json` (one JSON object per line, including the
  engine's messages), `--access-log` (every request: method, route, status,
  bytes, seconds, remote, token *name*), `--slow-op 1s` (default; WARN for
  slower requests, `0` turns it off; streams and long-polls exempt).
- Profiling: `--pprof` serves `/debug/pprof/` to admin tokens; `serve`
  refuses it without auth. Heap profiles can contain plaintext values.

`freshdb verify` (or `GET /v1/verify`) re-reads everything and reports
`"ok": true` plus counts. It is O(history); run it offline or rarely.
