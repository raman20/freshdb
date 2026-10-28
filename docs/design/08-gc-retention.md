# 08 — GC with retention: manifests, retired logs, history windows, journal space

Status: design (item 8). Builds on [03-compaction.md](03-compaction.md).

## Problem

Compaction (item 3) bounds segments and memory, but only by dropping *all*
history below the head, and several things still grow forever:

| what | grows with | today |
|---|---|---|
| `manifest/<id>.mf` | every open-for-write, branch op, index/ANN decl, compaction, sweep | never deleted |
| retired logs (old side of a rewind or promote) | every rewind/promote | kept in `Logs` forever; compaction even writes bases for them |
| reflog | every branch op | never trimmed |
| history | every commit | either all (no compaction) or none (compaction cuts at the head) |
| journal file `journal.fj` | every byte ever written | deleted records are hole-punched (disk blocks freed) but the file's size and its record headers stay; open scans them |

## What GC does

`(*DB).GC(ctx, GCOptions)`, `freshdb gc`, `POST /v1/gc`,
`freshdb serve --gc-every D`:

```go
type GCOptions struct {
    Grace         time.Duration // as Compact: replaced objects wait this long (CLI 0, API 5m)
    KeepFor       time.Duration // history window kept on every log (time travel, Rewind, CreateBranch, ViewLog)
    KeepCommits   uint64        // ... and at least this many newest commits per log
    KeepManifests int           // newest manifests kept (0 = 16; min 2)
    KeepReflog    int           // newest reflog entries kept regardless of age (0 = 64)
}
```

1. **Retention cut.** For each log, the cut `K` is the largest LSN that is
   both ≤ head − `KeepCommits` and has a commit time ≤ now − `KeepFor`
   (frame timestamps, the writer's clock), rounded down to a segment
   boundary. The base is written at `K` (a view folded to `K`), not at the
   head; segments after `K` stay. Everything at or above `K` keeps working:
   time travel, `Rewind`, `CreateBranch`, `Entries`, `Verify` from the
   base's chain head. With `KeepFor = KeepCommits = 0` this is exactly
   `Compact`.
2. **Branch dependencies.** An unbased branch reads its parent's frames up
   to its branch point `P`. So: a branch whose own cut would be ≤ `P` stays
   unbased (it still depends), and a log's cut is at most the smallest `P`
   of the branches that still depend on it (fixpoint over the lineage
   tree). This is the same state `CreateBranch` after a compaction already
   produces (branch point ≥ the parent's floor).
3. **Reflog pruning.** Entries newer than `KeepFor`, and the newest
   `KeepReflog`, are kept; older ones are dropped. `RefChange` gains a
   timestamp (older entries without one are pruned by count only).
4. **Retired logs.** A log is *reachable* if a ref points at it, a kept
   reflog entry names it (`From`/`To`), or a reachable unbased log depends
   on it. Unreachable logs leave the manifest; their segments, snapshots
   and base become garbage (same grace and sweep as compaction).
5. **Manifest GC.** After its own commit, GC deletes `manifest/<i>` for
   `i ≤ latest − KeepManifests`. Old manifests are only metadata history;
   nothing reads them except (a) `loadLatestManifest`, which picks the
   highest id, and (b) a *fenced* writer, whose create-only commit of
   `manifest/<its id + 1>` is the fencing check. Deleting manifests
   breaks (b): a zombie holding manifest `c` could now *create*
   `manifest/c+1` because the slot is free. Fix: **every manifest commit
   lists `manifest/` after the create and treats any higher id as
   "fenced"** (the stray manifest is deleted; it is never the latest, so
   no reader ever loads it). Since GC never deletes the newest manifests,
   any free slot below the latest implies a higher id exists. Cost: one
   List per manifest commit (rare: branch ops, opens, compactions).
6. **Journal space.** Holes already return the bytes of deleted objects to
   the file system (`du` shrinks); the file's apparent size and the dead
   record headers do not. `freshdb gc --shrink` (local journal, offline)
   rewrites `journal.fj` with only the live records into
   `journal.fj.shrink`, fsyncs it and renames it over the original (then
   fsyncs the directory). A crash before the rename leaves the old file
   (the temp file is removed on the next shrink/open); after it, the new
   file is complete. Exclusive use is enforced by a new lock file
   (`journal.lock`): every `Journal` holds a shared `flock` on it for its
   lifetime and shrink needs an exclusive non-blocking one, so it refuses
   while any (new-binary) process has the directory open. Binaries from
   before this change do not take the lock; do not run them during a
   shrink.

## Safety argument (model-checked: spec/FreshGC.tla)

- G1 **never delete anything reachable**: the objects needed by the latest
  manifest (bases, segments after each base, a dependent branch's parent
  prefix, the latest manifest itself) always exist.
- G2 **retention**: after GC, every kept log can still be read at any LSN
  in its window (the base is ≤ the cut).
- G3 **fencing survives manifest deletion**: a fenced writer never acts on
  a manifest that is not the latest (with the post-create List). The
  mutant without the check violates G1 (TLC trace in RESULTS.md).
- G4 garbage is final (as compaction's C3); readers holding an old
  manifest are covered by the grace assumption (FreshCompact C4).

## Not covered / limits

- Retention by time uses frame timestamps (writer clocks). A clock that
  jumps backwards keeps more history, never less than `KeepCommits`.
- Shrink is offline only (a live shrink would need every process to
  switch files atomically).
- Topics still keep every message (offsets stay dense); a base stores the
  messages up to the cut. Topic retention is not part of this item.

## Test plan

- Unit: cut selection (count, time, segment boundary), dependency fixpoint,
  reachability, reflog pruning, manifest GC + stray-manifest fencing
  (a zombie whose `c+1` slot was deleted gets `ErrFenced`).
- Property: random ops / branches / rewinds / GCs + reopen equal an
  un-GC'd twin at every LSN inside the window; `ErrCompacted` below it.
- Crash: stop after each GC step (base write, manifest commit, manifest
  deletion, sweep) and reopen; kill -9 of a child running GC in a loop;
  shrink: crash before/after the rename (hook), reopen and compare keys.
- Sim: GC with random retention in the simulator's op mix.
- TLA+: FreshGC.tla (retention cut, a dependent branch, rewind/reflog,
  log drop, manifest deletion, a zombie writer); mutants: no post-create
  check, ignoring dependents, deleting the latest manifest.
- Bench: `freshdb-bench gc`: N cycles of overwrites + branch/rewind churn
  with GC each cycle; disk allocated, apparent size, object and manifest
  counts per cycle, before (compact only) vs after (gc, gc + shrink).
