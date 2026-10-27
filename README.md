# freshDB — one log, every data model

freshDB is an embeddable (and servable) storage engine built on **one append-only
log** kept as immutable objects on S3-compatible storage (or a local
directory). The KV store, topics, consumer groups, documents, vectors and
transactions are not separate engines. Each is a **lens**: a pure fold over
the log. Because everything is the log, some features fall out almost for free:

- **Undo / time travel:** read any key as of any LSN, or rewind a branch.
- **Branches:** a branch is (parent log, LSN), which makes it copy-free.
- **Exactly-once processing:** consume a message, write results and ack, all in one serializable transaction.
- **Forget:** crypto-shred a subject. Its per-subject key is destroyed, so its
  data becomes unreadable on every branch, under time travel and in backups.
  You get a signed certificate, and the hash chain still verifies.

The whole thing is a single static Go binary (pure Go, no CGO), configured by one URL.

> Status: **prototype** (built in hours). The correctness core is model-checked
> and simulation-tested (see [Correctness](#correctness)). Performance and
> scale work have not started. Read [Limitations](#limitations) before relying on it.

## 5-minute quickstart

```sh
go install github.com/raman20/freshdb/cmd/freshdb@latest   # or: go build -o freshdb ./cmd/freshdb

# Embedded: the CLI opens the database directly (default ./freshdb-data)
freshdb put greeting "hello"
freshdb get greeting                       # hello
freshdb put greeting "hello, world"
freshdb get greeting --at 1                # hello   (time travel)
freshdb log                                # the raw log: every lens is a fold of this

# Topics + consumer groups
freshdb pub events "signup alice"
freshdb sub events --group mailer          # fetch + ack atomically

# Branch, experiment, throw away or promote
freshdb branch create exp
freshdb put greeting "experimental" --branch exp
freshdb get greeting                       # hello, world (main is untouched)
freshdb branch promote exp main

# Undo
freshdb rewind main 2                      # main back to LSN 2 (old head kept in reflog)

# Forget a person everywhere
freshdb put user/alice/email a@x.com --subject alice
freshdb shred alice                        # prints a signed certificate
freshdb get user/alice/email               # error (404)
freshdb verify                             # CRCs, hash chains, fencing, shred proofs
```

### One URL

| URL | Store |
|---|---|
| `./data` or `file:///abs/path` | local directory (atomic create-only via `renameat2(NOREPLACE)`/`link`, fsync) |
| `s3://bucket/prefix?endpoint=host:9000&insecure=1&create=1` | any S3 with conditional writes (`If-None-Match: *`) — AWS S3, MinIO, … |
| `mem://` | in-memory, for tests |

Set it with `--db URL` or `FRESHDB_URL`. S3 credentials come from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`.

### Server mode

```sh
FRESHDB_URL='s3://my-bucket/freshdb?endpoint=127.0.0.1:9000&insecure=1' freshdb serve --listen 127.0.0.1:7070
export FRESHDB_ADDR=http://127.0.0.1:7070   # now every CLI command talks to the server
freshdb put k v
curl -X PUT  localhost:7070/v1/kv/k -d v
curl         localhost:7070/v1/kv/k?at=1
curl -X POST localhost:7070/v1/tx -d '[{"op":"put","key":"a","value":"1"},{"op":"pub","topic":"t","value":"m"}]'
```

The CLI and server share one code path: in embedded mode the CLI calls the
same HTTP handler in-process. **Opening a database takes the write lease and
fences every other writer.** If a server is running, embedded CLI commands on
the same URL will fence it, so use `--addr`/`FRESHDB_ADDR` instead.

HTTP API (JSON; `?branch=`, `?subject=` for writes, `?at=` for time travel):

| | |
|---|---|
| `GET/PUT/DELETE /v1/kv/{key}`, `GET /v1/kv?prefix=` | KV |
| `POST /v1/tx` | atomic batch: `put del pub ack doc vec` ops |
| `POST /v1/topics/{t}`, `GET /v1/topics/{t}?from=&max=`, `POST /v1/topics/{t}/groups/{g}/next` | topics, groups |
| `PUT/GET /v1/docs/{c}/{id}`, `GET /v1/docs/{c}?field=&value=`, `POST /v1/indexes` | documents + secondary indexes |
| `PUT/DELETE /v1/vectors/{c}/{id}`, `POST /v1/vectors/{c}/search` | vectors (cosine) |
| `GET/POST /v1/branches`, `POST /v1/branches/{b}/rewind`, `POST /v1/branches/{b}/promote` | branches |
| `GET /v1/log`, `POST /v1/shred/{subject}`, `GET /v1/verify` | log, forget, audit |

Errors: 404 not found, 409 conflict, 410 subject shredded, 503 fenced by a newer writer.

### Go API

```go
db, _ := freshdb.Open(ctx, "s3://bucket/app?endpoint=...")
db.Update(ctx, func(tx *freshdb.Tx) error {
    g := tx.Topic("tasks").Group("worker")
    msgs, err := g.Fetch(1)
    if err != nil || len(msgs) == 0 { return err }
    tx.Put("result/1", []byte("done"), freshdb.Subject("alice"))
    return g.Ack(msgs[0])                     // result + ack: all-or-nothing
})
db.Branch("main").ViewAt(ctx, 42, func(tx *freshdb.Tx) error { ... })
cert, _ := db.Shred(ctx, "alice")
```

## Demo: an agent with undo, memory and forget

[`examples/agent-demo.sh`](examples/agent-demo.sh) starts a server and plays an
AI agent through these steps:

1. The agent stores memories about Alice in KV, documents and vectors.
2. It processes tasks from a topic with exactly-once acks.
3. It "goes rogue" on main, and is undone with time travel and rewind.
4. It tries a plan on a branch and promotes it.
5. Alice asks to be forgotten: `shred alice`.

```sh
go build -o /tmp/freshdb ./cmd/freshdb && FRESHDB=/tmp/freshdb ./examples/agent-demo.sh   # needs jq, curl
```

The full output from a real run is in [`examples/agent-demo.out`](examples/agent-demo.out). The ending:

```
== 5. Forget: alice invokes her right to erasure — crypto-shred subject 'alice'
{"valid_signature":true,"shred_lsn":12,"dek_deleted":true,"chain_head":"8c2b5cf3c64dcacb...","signature":"2ebac16f502fa837..."}
-- KV:
error (404): freshdb: not found
-- time travel:
error (404): freshdb: not found
-- old branch:
error (404): freshdb: not found
-- docs about alice:
[]
-- vector recall [1,0,0]:
{"id":"m2","score":0.11043153}
-- bob is untouched: {"user":"bob","text":"bob prefers window seats"}
-- new writes for alice are refused:
error (410): freshdb: subject has been crypto-shredded

== 6. Verify the whole history (CRCs, hash chains, fencing, shred proofs)
{"ok":true,"logs":3,"segments":17,"entries":14,"encrypted_values":7,"unreadable_shredded_values":5,"problems":null}
-- grep the raw bucket for alice's plaintext:
no plaintext found in /tmp/agent-demo-data
```

## How it works

```
bucket/
  manifest/00000000000000000007.mf   create-only, dense ids; epoch + branch refs + reflog
  log/<logID>/00000000000000000042.seg  one immutable object per group commit
  snap/<logID>/...                    optional KV/topic checkpoints (an optimisation, never the truth)
  keys/<subject-hash>                 per-subject data keys (DEKs), wrapped by the KEK
```

- **Writes:** transactions are validated optimistically (OCC, serializable) and
  group-committed into one segment object with a create-only PUT. The write is
  acked only after the PUT succeeds. Frames carry CRC32C and a SHA-256 hash chain.
  Values written with a subject are AES-GCM encrypted under that subject's DEK.
- **Fencing, without clocks:** `Open` creates manifest `id+1` with `epoch+1`
  (create-only, so exactly one opener wins), then writes a fence segment. A
  zombie writer's next create-only PUT collides, or it sees a newer epoch, and
  it stops with `ErrFenced`. Ambiguous PUT outcomes are resolved by reading
  back, or else reported as `ErrUnknownOutcome` and never acked.
- **Lenses** (`lens/`) are pure folds over `redact(log, shredded)`.
  `lens/purity_test.go` forbids I/O imports there. A property test checks
  `lens(state) == fold(log)` after arbitrary programs, restarts and snapshots.
- **Branches:** a log has a parent and a fork LSN. Rewind forks a new log and
  swaps the ref, and the old head goes into the reflog, so nothing is deleted.
- **Shred:** write a marker, delete the DEK, append an audit entry, then issue an
  ed25519-signed certificate (subject hash, LSN, chain head). Ciphertext stays,
  so the hash chain still verifies, but it is unreadable.

## Correctness

The method is formal models, then a deterministic simulation against a reference
model, then property tests, then crash and fault tests on real backends. Results
are from this box, 2026-10-27.

**TLA+ / TLC** ([`spec/`](spec), details in [`spec/RESULTS.md`](spec/RESULTS.md)).
Every run explored its complete bounded state space with no errors:

| Spec | Distinct states | Checks |
|---|---:|---|
| FreshLog | 2,475,220 | durability, no holes, epoch monotone (no split brain), no double-apply, torn tail only unacked, immutability |
| FreshTxn | 2,421,454 | exactly-once, atomic cursor, serializable, durable prefix |
| FreshBranch | 1,845,287 | rewind exact, old head kept, append-only, time travel stable |
| FreshShred | 6,481 | shredded never revealed, no writes after shred, chain verifiable, key eventually gone (liveness) |

TLC found a **real bug**: a slow opener could append an older-epoch fence after
a newer one, after which the next open refused the log as corrupt (bricked).
It is fixed in `log.go`, and `TestConcurrentOpenersNeverBrickLog` reproduces it.
Three deliberate mutations, each checked to be caught, are listed in RESULTS.md.

**Deterministic simulation** (`sim/`). It uses a seeded workload, a fake clock,
seeded randomness, and a fault-injecting store (failed, lost, delayed and torn
PUTs, plus unknown outcomes). Each run mixes crashes, takeovers by zombie
writers, shreds and rewinds. After every step, recovered state is checked
against a reference model: `acked ⊆ recovered ⊆ acked ∪ ambiguous`, strict
order, lenses equal the model fold, and `Verify` passes.
`TestDeterminism` checks that the same seed gives the same trace hash.

- 50,000 seeds before the final fixes; 10,000 seeds on the final code
  (137,293 acked txns, 75,537 crashes, 29,979 takeovers, 114,926 injected faults): all OK.
- Reproduce one seed: `go test ./sim -run TestSimulation -seed=N`. Run more: `-seeds=N`.

**Property tests** use [rapid](https://pgregory.net/rapid). Checked properties:
fold determinism, snapshot+tail == fold, `DropSubject == fold(redacted log)`,
lenses refine simple models (MVCC KV, topics, brute-force vectors, doc index),
and engine-level lens digests == rebuild after random programs with branches,
rewinds, shreds and reopen.

**Crash test:** 200 rounds of `kill -9` during 4 concurrent writers. Checked:
no lost acks, atomicity, prefix consistency, no double-apply, and Verify.

**Fuzzing** covers the segment and payload decoders. Fuzzing found that
non-minimal varints were accepted, so the decoder is now canonical.

**Regression tests for the previous prototype's bugs:** stale reads after restart, deleted
vectors returned by search, vectors lost on restart, and a torn file blocking open.

```sh
go test -race ./...                                  # everything (S3 test skipped unless FRESHDB_TEST_S3 is set)
FRESHDB_TEST_S3='s3://freshdb-test?endpoint=127.0.0.1:9000&insecure=1&create=1' go test ./...   # + MinIO/S3
spec/run_tlc.sh                                      # TLC (needs Java + tla2tools.jar)
```

## Benchmarks (this box only — not a performance claim)

8-vCPU Xeon VM, ext4 virtual disk, Go 1.27, `go test -bench . -benchtime 2s`.
These are single runs with no tuning, from a prototype, and **no S3 numbers**.
Expect S3 commit latency to be dominated by the PUT round trip (≈tens of ms).

| Benchmark | Result |
|---|---|
| Put, 1 writer, 1 commit/put, fs (fsync) | 275 µs/op (≈3.6k commits/s) |
| Put, 1 writer, mem | 84 µs/op |
| Put, 16×GOMAXPROCS writers (group commit), fs | 9.6 µs/op (≈105k puts/s) |
| Put, parallel, mem | 4.9 µs/op |
| Put, parallel, encrypted per-subject (mem) | 6.7 µs/op |
| Get (10k keys, mem) | 377 ns/op |
| Vector search, 10k × 128-d, brute force, k=10 | 3.0 ms/op |
| Cold open of 10k keys / 100 segments (fs), no snapshot | 8.1 ms |
| … with snapshot | 9.4 ms (recovery still scans and verifies every segment, so the snapshot does not help yet) |

## Limitations

These are known and honest:

- **All lens state lives in RAM.** Open replays the log. Snapshots exist for KV
  and topics, but recovery still reads every segment. There is no compaction or
  segment merging yet, and object count grows by one per group commit (plus
  manifests and fences per open).
- **Vector search is brute force.** There is no ANN index.
- **Single writer per database.** Fencing is by epoch and conditional writes,
  with no lease or timer, so a new opener always wins. Each embedded CLI call is
  an open, which fences a running server (use `--addr`).
- **Not in the OCC read set:** vector and doc-index reads. Only KV, topic and
  cursor reads are validated, so a txn that searches vectors and then writes is
  not serializable with respect to those reads.
- **Rewind races:** commits in flight while a branch is rewound land on the
  retired log, which is still kept and reachable via the reflog.
- **HTTP group consume** (`/groups/{g}/next`) fetches and acks in one txn, so it
  is *at-most-once* for the client. Exactly-once needs the result write to be in
  the same txn (Go API or `/v1/tx` with an `ack` op).
- **Shred window:** if a DEK-create PUT is delayed and lands after the shred,
  the orphan DEK exists until the next open or `verify` sweeps it. The sim found
  this case, and the FreshShred spec checks it with liveness. No plaintext is
  ever readable through freshDB in that window.
- **KEK in dev mode** is stored in the bucket (`keys/_kek`). The Go API takes
  `WithKEK` (e.g. from a KMS), but the CLI has no flag for it yet. Bucket
  backups that include the KEK can recover undestroyed DEKs, but never shredded ones.
- **What the checking does not cover:**
  - The specs are bounded and hand-matched to the code; nothing is proven
    equivalent to the code.
  - The sim does not interleave two concurrent `Open`s; that case is covered by
    TLC plus a dedicated test.
  - `kill -9` does not model power loss. Torn writes are injected separately.
- No auth or TLS on the HTTP server (it binds 127.0.0.1 by default), no
  replication or read replicas, no multi-database transactions.

## Layout

```
*.go          engine: open/fencing (db.go, log.go), writer + group commit, txns (tx.go),
              branches, shred/verify, keyring
format/       segment + payload encoding (CRC32C, hash chain), fuzz targets
lens/         pure folds: kv (MVCC), topic, doc+index, vector; property tests
store/        Store interface: fs, s3, mem, faulty (fault injection)
sim/          deterministic simulator
server/       HTTP API
cmd/freshdb/     the single binary
spec/         TLA+ specs, TLC runner, RESULTS.md
examples/     agent demo
```

## License

Apache-2.0. See [LICENSE](LICENSE).
