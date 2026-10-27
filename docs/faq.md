# FAQ

**Is freshDB production-ready?**
No. v0.1.0 is a prototype. Its correctness core is model-checked and
simulation-tested, but it has no production users, no compaction, and its
lens state lives in RAM. See the README's status section.

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
mature for KV workloads and has compaction. See the comparison table in the README.

**Which platforms are supported?**
Linux is the primary platform: all tests, the simulator and the crash tests run
there. In CI the unit tests also run on macOS. Windows binaries build, but are
untested: Windows can't fsync a directory, so the durability of a new file's
directory entry after power loss is not verified there.

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

**How fast is it?**
See the benchmarks in the README. They are from one VM on local disk and are
not a performance claim. On S3, commit latency is dominated by the PUT round
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
