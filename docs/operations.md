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
back with ranged GETs.

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
- every KV key and its full version history (MVCC; history is never
  trimmed yet), with values of up to 64 B inline;
- topic messages, documents and doc indexes, and vectors;
- the **value cache** for KV values over 64 B, which are otherwise read
  from the log (`pread` locally, ranged GET on S3) and CRC-checked.

The value cache is off the Go heap (one anonymous `mmap` arena on Unix), so
RSS grows by about its size once it fills. Go API options:

| Option | Default | Effect |
|---|---|---|
| `WithValueCacheBytes(n)` | 256 MiB | cache size; negative disables the cache |
| `WithInlineMax(n)` | 64 B | KV values up to `n` bytes stay in RAM; negative keeps none inline |
| `WithSnapshotEvery(n)` | 0 (off) | KV+topic snapshot every `n` commits; written synchronously and does not speed up open yet |

The CLI and `freshdb serve` use the defaults; they have no flags for these
yet. For scale: the 1M × 1 KiB YCSB run peaked at 2.12 GiB RSS with the
defaults on the journal layout (0.99 GiB on the files layout with the old
64 MiB cache; v0.1.0: 7.1 GiB). Lower the cache for small machines, raise it
if your hot set is larger and reads miss.

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
- The server has **no auth or TLS** and binds to `127.0.0.1` by default.
  If you expose it, put an authenticating TLS proxy in front.
- To scale, shard by database (one per tenant or agent).

## Backups and restore

- **Local, journal or files layout:** stop the writer, then copy the
  directory (`cp -a data data.bak`). Check a copy with
  `freshdb --db ./data.bak verify`.
- **S3:** segment, manifest and certificate objects are immutable, so a copy
  of the prefix is a complete backup; `keys/dek/*` objects are deleted on
  shred.
- **Shred and backups:** a backup (or an object version) taken before a
  shred still holds that subject's DEK and can decrypt its data. If you
  rely on shred, expire noncurrent versions of `keys/` and rotate backups.
  In dev mode the KEK is in `keys/_kek` too. See [security.md](security.md).
- **Restore:** copy the backup to a new location and open it. `verify`
  checks CRCs, hash chains, fencing and shred state.

## Growth and housekeeping

There is **no compaction or retention yet**: the log only grows. In the
files layout and on S3 that is one object per group commit plus a manifest
and a fence per open; the journal is one file that grows in 8 MiB steps.
Retired logs from `rewind` stay reachable through the reflog. Plan disk
for the full history.

## Health checks

`freshdb verify` (or `GET /v1/verify`) re-reads everything and reports
`"ok": true` plus counts. It is O(history); run it offline or rarely.
