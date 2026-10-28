# FAQ

**Is freshDB production-ready?**
No. It is a pre-1.0 prototype (latest release v0.1.0; `main` has unreleased
improvements). Its correctness core is model-checked and
simulation-tested, but it has no production users, only a first version of compaction, a single
shared-token auth, and most lens state lives in RAM. See the README's
[status section](../README.md#status-and-limitations).

**Why not just Postgres?**
Postgres is a great default, and if it works for you, keep it. freshDB is for
when you want what a log gives you natively: branches, rewind, time travel,
exactly-once consume-and-write, and per-subject deletion that also covers
history and derived indexes. And you want them on object storage, from one
binary, without running a server cluster.

**How is this different from SlateDB?**
SlateDB is an embedded LSM KV store on object storage, written in Rust. It
inspired freshDB's fencing protocol. freshDB is pure Go, and it puts topics,
documents, vectors and crypto-shredding on the same log. SlateDB is far more
mature for KV workloads and has compaction. See the [comparison table](../README.md#how-it-compares) in the README.

**Which platforms are supported?**
Linux is the primary platform: all tests, the simulator and the crash tests run
there. In CI the unit tests also run on macOS. Windows binaries build (a
build break after v0.1.0 is fixed on `main`) but are untested: Windows
can't fsync a directory, so the durability of a new file's directory entry
after power loss is not verified there. Windows uses the files layout; the
journal layout is Unix-only.

**I have a v0.1.0 data directory. Do I need to do anything?**
No. It keeps the files layout (one file per segment) and opens as before.
To move it to the faster journal layout on Linux, macOS or BSD:
1. Stop every process using it.
2. Run `freshdb migrate --db DIR` (from `main`; v0.1.0 has no `migrate`).

`migrate` copies and verifies every object and leaves the original as
`DIR.bak-files`. If it is interrupted, run it again and it finishes the
job. `freshdb migrate --to files` converts back.

**Can two processes write to the same database?**
Only one at a time. A new opener fences the old one, which then gets
`ErrFenced`. For many writers, use many databases (for example one per tenant
or agent), or run `freshdb serve` and send writes through it.

**What happens if my process crashes mid-commit?**
Acked transactions survive. A transaction that wasn't acked either fully
appears after recovery or doesn't appear at all. A torn final segment is
repaired to its valid prefix, and the damaged original is kept in
`quarantine/`. This is checked by the FreshLog spec, the simulator and the
kill -9 crash test.

**What exactly does shred guarantee?**
After `Shred(s)` returns, freshDB will not return any value written with
subject `s`. That holds on every branch and at every LSN (time travel),
because the only key that decrypts those values has been deleted. The log
bytes stay, so the hash chain still verifies. You get a signed certificate.
See *Guarantees* in the README for the exact limits: keys and unsubjected
values are not covered, and the dev-mode KEK caveat applies.

**Does shred work with S3 versioning or backups?**
Ciphertext in old object versions is useless without the DEK. **But if your
bucket keeps old versions of `keys/dek/*`, or a backup copied them, the DEK
still exists there.** Expire noncurrent versions of `keys/`, and back up with
that in mind. KMS-held keys are on the roadmap.

**How much memory does it need?**
Keys, KV history, topics, documents and vectors are in RAM; KV values over
64 B are read from the log through a 256 MiB off-heap cache
(`WithValueCacheBytes`). The 1M × 1 KiB YCSB run peaked at 2.12 GiB on `main`
(7.1 GiB on v0.1.0). Old KV versions stay in memory until you run
`freshdb compact` (new on `main`), which keeps only the latest live version
of each key; topics keep every message. See
[operations.md](operations.md#compaction). See [operations.md](operations.md#memory-and-cache-sizing).

**How fast is it?**
See [benchmarks.md](benchmarks.md) and the summary in the README. They are
single runs on one shared VM on local disk and are not a performance claim. On S3, commit latency is dominated by the PUT round
trip. Group commit amortises it across concurrent transactions.

**Which object stores work?**
Anything with create-only conditional writes (`If-None-Match: *`). **Only
MinIO is tested** (in CI, on every push). AWS S3 has supported `If-None-Match`
on PUT since August 2024, and Cloudflare R2
[documents atomic `If-None-Match: *`](https://developers.cloudflare.com/r2/reference/consistency/).
Both should work, but we have not tested them yet; reports are welcome. Stores
without create-only semantics cannot be opened for writing.

**Why "lenses"?**
Because they don't store anything of their own. They are ways of looking at the one log.
