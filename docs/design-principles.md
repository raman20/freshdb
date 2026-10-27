# Design principles

freshDB exists because a typical app or agent backend now runs five or six
systems: Postgres for state, Redis for cache and locks, Kafka/SQS for events,
a vector database for memory, an outbox table to keep them consistent, and a
pile of scripts for backups and GDPR deletes. Each one has its own failure
modes, its own backup story and its own deletion story.

freshDB's bet is that **one log, read through several lenses, is simpler than
six systems** for small and medium workloads. Every principle below exists to
protect that simplicity.

## 1. One log is the only truth

Every write — KV, topic message, document, vector, consumer-group ack, shred
marker — is a record in one append-only, hash-chained log. Everything else is
derived and can be thrown away and rebuilt:

```
state(lens) == fold(apply, empty, redact(log, shredded))
```

This equation is checked by property tests and by the simulator on every run.
New features must preserve it. If a feature needs state that cannot be
rebuilt from the log, it needs a very good reason.

## 2. Lenses are pure

A lens may not read clocks, randomness, files or the network. All it sees is
the log entry and a deterministic environment. `lens/purity_test.go` enforces
this with an import allow-list. Purity is what makes replay, time travel,
branches and "add an index later" correct by construction.

## 3. Object storage is the only coordination primitive

There is no consensus protocol, no ZooKeeper and no lease timer. Fencing comes
from create-only writes (`If-None-Match: *` on S3; `renameat2(NOREPLACE)` on
local disk) plus epochs. If your store can't do create-only writes, freshDB
refuses to open it for writing.

## 4. One binary, one URL

`freshdb` is a single static, CGO-free binary. Configuration is one URL. New
knobs need a benchmark or a bug report that justifies them.

## 5. Correctness is a feature, so it is shown, not claimed

Protocol changes come with a TLA+ model update and a TLC run. Engine changes
come with simulator coverage. The results are in the repo
([spec/RESULTS.md](../spec/RESULTS.md)), and CI re-runs them on every push.
We never say "production-ready" until the evidence supports it.

## 6. Honest by default

The README lists what is *not* guaranteed next to what is. Benchmarks say which
box they ran on. Estimates are labelled as estimates.

## What we refuse to build

These are deliberate, so please read them before you open a feature request.
Some may be revisited with strong evidence; the default answer is no.

- **Multi-writer / multi-master per database.** There is one writer per
  database. Scale out by sharding *databases* (one per tenant or agent), not by
  distributed consensus.
- **Cross-database transactions.**
- **A custom consensus protocol or Raft.** Object-storage conditional writes are
  the only coordination primitive. (Read replicas that tail the log are on the
  roadmap because they need no consensus.)
- **A full SQL engine inside the core.** SQL may arrive as a separate layer or
  through an export (Parquet/Iceberg) that DuckDB and friends can read.
- **Server-side user code:** no stored procedures, triggers or UDFs.
- **The Kafka wire protocol in the core.** A compatibility shim, if ever,
  lives in a separate project.
- **A graph query language, or joins across lenses** beyond what a Go loop can do.
- **"AI features" inside the database.** You bring the embeddings; freshDB
  stores and searches them.
- **Tunables without evidence.** Every knob costs every user some
  understanding.
