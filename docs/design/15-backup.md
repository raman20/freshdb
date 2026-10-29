# 15. Backup, restore, point-in-time (item 16; gaps S3.5, S3.6)

## Problem

The only documented backup was "stop the writer and copy". A copy of a
live database can be torn: compaction writes a new base and manifest and
then deletes the segments the old manifest referenced, so a copy that took
the old manifest but reached the segments after the sweep is missing
objects, and nothing would notice until restore.

## Design

**Delete hold.** `DB` wraps its store in `holdStore`. While any backup
holds it, `Delete` calls are queued and applied when the last hold is
released (best effort: a failed deferred delete leaves garbage that the
next compaction/GC sweeps). A `PutIfAbsent` of a queued key applies its
delete first (caches such as snapshots can be re-created under the same
key). Deletes under `keys/` (shred) are never held: shredding must not
wait on a backup.

**Copy order.** (1) delete DEST's old `backup/info.json`; (2) list
`manifest/` (T0); (3) list every object (T1 ≥ T0); (4) copy all
non-manifest objects (8 workers), skipping keys DEST already has; (5) copy
the T0 manifests; (6) remove DEST objects the source no longer has; (7)
sync; (8) read DEST back and write `backup/info.json` with object count,
bytes and a SHA-256 over (key, SHA-256(value)) in key order.

Why it is consistent: every object the newest T0 manifest references was
written before that manifest and is not deleted before step 8 (hold).
Segments are written before they are acknowledged and in sequence order,
so the T1 listing holds, per log, a gap-free run of segments that extends
the manifest; recovery replays them. Objects of logs created after T0
are unreferenced garbage in the backup. Manifests are copied last, so a
crash during the copy never leaves a manifest pointing at missing data;
without the record the backup is refused anyway.

**Incremental.** Object keys are immutable in this design except caches,
so "key exists in DEST" means "same bytes" for authoritative objects;
step 6 makes DEST a mirror (and removes shredded keys). A DEST that is not
empty must hold manifests and the same `keys/_kekcheck`/`keys/_kek` as the
source, else it is refused.

**Restore** requires the record, an empty destination, copies data then
manifests, re-reads everything and checks count, bytes and digest
(`ErrCorrupt` on mismatch), then the CLI opens the database and runs
`Verify`. `--at LSN` = `Rewind` after restore: PITR is bounded by the
history the backup holds (compaction/GC retention).

## Tests

- `TestBackupHoldsDeletes`: backup blocked mid-copy, a compaction deletes
  objects; nothing disappears until the backup ends, then the deletes
  apply; restore is correct. Mutation: disabling the hold fails it.
- `TestBackupDrill` (mem and journal): sequential writer + compaction/GC
  every 2 ms; restore is a gap-free prefix ⊇ writes acked before the
  backup; incremental backup after a shred copies a fraction and drops the
  subject's key.
- `TestBackupFailures`: interrupted backup, interrupted update of a good
  backup, 1-bit corruption, non-empty destination, another database's
  backup.
- `TestRestorePITR`; `scripts/restore-drill.sh` (real binary, kill -9).

No TLA+ model: the argument rests on two invariants already in the model
of item 3 (segments precede their acknowledgement; sweeps only delete
objects unreachable from the newest manifest) plus the hold, which is
tested directly.

## Not done

- Scheduled backups and retention slots (use cron + one DEST per slot).
- Cost of S3 destinations not measured.
- `freshdb backup` without a server opens the database (fencing a server
  on the same URL, like every embedded CLI command); use `--addr`.
