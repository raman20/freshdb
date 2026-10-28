# 03 — Compaction / GC (item 3)

## Goal
Bound storage (and the memory spent on MVCC history) by dropping data no
reader at the current head can see: overwritten and deleted values, and
values of shredded subjects. freshDB has no TTL, so "expired" data does
not exist yet; when TTL lands, compaction is where it is dropped.

## What compaction keeps and drops
`Compact` writes, for every log in the manifest, a **base image** of the
state at the log's head `K`:
- KV: the latest version of every live key (deletes and older versions
  dropped; redacted / shredded versions dropped).
- Topics: every message, so offsets stay dense; shredded ones stay as
  redacted placeholders without bytes.
- Subject (crypto-shredding) values are re-sealed with the based log as
  associated data; values of subjects shredded later are dropped by the
  next compaction (until then they are unreadable ciphertext, as today).
- Vector and doc-index lenses are rebuilt at open from the base's `vec:*`
  and `doc:*` keys, in original commit order.

A based log no longer needs its parent: its base is a full image, so
compaction removes structural sharing between branches (each branch's
base holds its own copy). Everything a log had at LSN ≤ K becomes
garbage: segments, the old base, KV snapshots.

**Lost by compaction (documented, intended):** time travel, `Rewind`,
`CreateBranch` and `ViewLog` at an LSN below the base (they return
`ErrCompacted`); `Entries`/`Verify` start at the base (the base records
the hash-chain head at K, so the chain stays verifiable from there).

## Storage layout
- `base/<log>/<K>-e<epoch>.base`: header (magic `FRDBBAS1`, K, TS at K,
  chain head at K), the KV image (lens KV snapshot encoding, refs point
  into value chunks), the topic image, a trailer with section offsets and
  a CRC-32C over everything but the value chunks.
- `base/<log>/<K>-e<epoch>/<n>.val`: value chunks (≤ 64 MiB each); a KV
  version that lives there has `Chunk = n+1` and `Off` inside the chunk.
  Every value keeps its own CRC-32C (`Version.Sum`), checked on read.
- `Manifest.Logs[id].Base = {LSN, Seq, Key, Chunks, Chain, TS}`; `Seq` is
  the first segment sequence not covered. `Manifest.Garbage` lists
  objects (and prefixes) to delete, each with a not-before time.

## Protocol (model-checked: spec/FreshCompact.tla)
Compaction runs on the writer (the epoch holder) with commits paused:
1. Sweep due garbage (see 5).
2. For each log: write value chunks, then the base object (create-only).
3. Commit one manifest that sets every `Base`, clears `SnapKey`, and
   appends to `Garbage`: covered segments, old bases, old snapshots
   (not-before = now + grace).
4. Rebuild the in-memory state from the new bases (writers and views are
   replaced; commits resume).
5. Sweep: delete garbage whose not-before has passed; the next manifest
   commit drops entries that are gone. Base objects of an older epoch
   that no manifest references (a crashed or fenced compaction) are
   deleted too; objects of our own or a newer epoch never are.

Why it is safe:
- C1 (no loss): the manifest that stops referencing an object is
  committed only after the base replacing it is durable; a crash before
  step 3 leaves the old manifest fully intact (the new base is orphaned).
- C2 (fencing): the manifest commit is create-only on `ID+1`, so a fenced
  (zombie) compactor cannot commit; it may only leave orphans or delete
  objects already listed as garbage, which are never referenced again.
- C3 (garbage is final): keys are never reused (bases carry K and epoch,
  segment sequences only grow), so nothing listed as garbage is
  referenced by a later manifest.
- C4 (readers): an instance still reading through an older manifest
  (a fenced writer, or an in-flight read during step 4) finds its objects
  until the grace period passes. This is a timing assumption: a reader
  holding a manifest longer than the grace can get `ErrNotFound`, never
  wrong bytes (all reads are CRC-checked).

## Failure modes
- Crash at any step: recovery opens the latest manifest; orphans and due
  garbage are swept at the next compaction. Tested by stopping after every
  step and by kill -9 of a compacting child.
- Live set > what fits in memory: values are streamed chunk by chunk; the
  KV image itself is in memory (it is already, as the lens).
- Base image corrupt: open fails with `ErrCorrupt` (no silent fallback,
  the old segments may be gone).

## Interfaces
- `(*DB).Compact(ctx, CompactOptions{Grace})` and `freshdb compact <url>`
  (`--grace`, default 0 for the offline CLI, 5 min for the API).
- Background trigger: `freshdb serve --compact-every <dur>`.

## Test plan
- Unit: base encode/decode, chunking, ErrCompacted paths.
- Property: random ops + compactions + reopen equal a model (all spaces,
  topics, subjects, shreds, branches).
- Crash: stop after each protocol step and reopen; kill -9 child.
- Sim: compaction steps in the deterministic simulator with faults.
- TLA+: FreshCompact.tla (crashes, a fenced compactor, a slow reader).
- Bench: storage bytes and open time before/after compaction for an
  overwrite-heavy workload; YCSB unaffected.

## Outcome (commits b873ae3, 09773ea)
- Implemented as designed. TLC found a same-key base reuse bug in the model
  before any code (fixed by the epoch + nonce in the base key).
- Tests: unit/ErrCompacted paths on mem, fs and journal; a rapid property
  test (random ops + compactions + reopen equal an uncompacted twin, 2,000
  checks); a failing-store test at the manifest commit and the sweep;
  fenced compactor; grace with a fake clock; a storage bound test (8
  overwrite + compact cycles: data objects and bytes stay flat); kill -9 of
  a child compacting every 5 ms (fs and journal); 2,000 sim seeds with
  4,190 compactions under injected faults.
- Mutations caught: dropping topics from the base; the wrong re-seal AAD.
- Bug found after the first push: the orphan sweep parsed the epoch with
  `LastIndex("-e")`, so a base whose hex nonce starts with `e` (1 in 16)
  was never swept (a leak, not data loss). Fixed by splitting the key's
  fields; `TestBaseEpoch` and 500 reruns of the sweep test.
- Bench (`freshdb-bench compact`, 9.8 MiB live of 488 MiB written): journal
  disk 504 → 34 MiB allocated, open 1.6 s → 30 ms, heap 54 → 2.8 MiB; files
  layout 508 MiB / 5,004 files → 10.6 MiB / 9 files.
- Not done: manifest/fence collection (a stale fenced writer holding
  manifest k relies on `manifest/k+1` existing; deleting old manifests
  needs that argument in TLA+ first), compaction without pausing commits,
  shrinking the journal file, retention windows, and retired-log removal.
  There is no TTL feature, so there is no "expired data" to drop.
