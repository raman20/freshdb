# 12 — Durability: no silent data loss on corruption, crash or oversize input

Production-readiness item 13 (docs/PRODUCTION_READINESS.md, rows S1.x).

## Problem

The audit found several ways to lose acknowledged data without an error:

- **S1.1** A transaction over the 64 MiB frame limit was encoded and
  acknowledged anyway. Every later open decoded the frame as corrupt and
  refused (or, in the tail segment, "repaired" it away).
- **S1.2** HTTP bodies were cut at 64 MiB by `io.LimitReader` and the
  truncated value stored.
- **S1.3** The journal scan stopped at the first record failing its CRC and
  treated the rest of `journal.fj` as a torn tail: one flipped bit hid every
  later object, and the next append overwrote them.
- **S1.4** A damaged final segment was "repaired" (truncated to its valid
  prefix) automatically. Every store writes objects atomically, so such a
  segment is corruption, not a torn write, and may hold acknowledged commits.
- **S1.5** A segment with an intact header of an unknown version was treated
  as a torn header and could be replaced.
- **S1.6** A journal object whose bytes failed their CRC read as "not found".
- **S1.7** Losing the newest segments (e.g. a restore from an older copy)
  was not noticed.
- **S1.8** No power-loss test: kill -9 keeps the page cache, so it never
  exercises torn or reordered writes.
- **S1.9** `verify` did not read snapshots or unreferenced objects.

## Design

**Size limits (S1.1, S1.2).** `freshdb.MaxTxBytes` (64 MiB encoded, after
encryption) and `MaxTxOps` (2^20). The writer checks each transaction
after encoding its frame; an oversize one gets `ErrTooLarge` and nothing is
written (the rest of the batch commits). A group commit also stops adding
transactions once its segment passes 256 MiB (the journal takes objects up
to 1 GiB); the rest wait for the next segment. The server wraps every body
in `http.MaxBytesReader` (`--max-body`, default 96 MiB: one transaction plus
JSON/base64 overhead) and answers 413 for an oversize body or
`ErrTooLarge`.

**Journal recovery (S1.3, S1.6).** Open (under the exclusive flock) no longer
stops at the first bad record. It classifies each record failing its CRC:

| Damaged record followed by | Meaning | Action |
|---|---|---|
| nothing valid | torn append (crash mid-write) | ignore it, zero its bytes, fsync |
| a valid delete record of the same key | interrupted delete (crash during the dead-marking header write or hole punch) | finish the delete |
| any other valid record | corruption | `store.ErrCorrupt` with file, offset, key; file unchanged |

A damaged header can also hide the length of the record, so after the scan
the bytes past the end must be zeros; if not, every offset in them is tried
as a record start and a valid one is corruption, else they are zeroed as a
torn tail. Holes are skipped with `SEEK_DATA` on Linux. `Get` of a live
object that fails its CRC returns `ErrCorrupt` instead of not found.

**Engine (S1.4, S1.5, S1.7).** A damaged final segment is `ErrCorrupt`,
the segment untouched (open does write its new manifest first: that is the
fencing step and holds no data). `WithRepairTail(true)` / `--repair-tail`
keeps the old behaviour: copy to `quarantine/`, keep the valid prefix. The
simulator turns it on because its faulty store models non-atomic puts. A
header with a good CRC and an unknown version is `format.ErrVersion`
("written by a newer freshdb? upgrade the binary") and is never repaired. A
log that ends before its manifest snapshot's LSN is `ErrCorrupt` ("newest
segments are missing"); a child branch point past the parent's end already
was.

**Scrub (S1.9).** `Verify` additionally CRC-checks every snapshot (a
damaged one is a warning: snapshots are a cache, open replays the log
instead), and on local stores
(fs, journal, mem) reads every object back through `Get`, so a journal
record that rotted after open is reported. Quarantined objects are listed as
warnings. On S3 only referenced objects are read (`scrubbed: false`).

## Power-loss model (S1.8)

`store/powerloss_test.go` is a dm-log-writes-style crash-point enumeration
at the file level. A test hook (`jtrace`, nil in production) records every
write (hole punches as writes of zeros) and every fsync/fdatasync of
`journal.fj`, in order, while a random workload of puts (1 B to 70 KB),
deletes and reopens runs (open's own recovery writes are traced too). For
every interval between two syncs it builds the images a power cut there can
leave: all writes before the interval's opening sync, plus none, all, or a
random subset of 512-byte sectors of the writes inside it (torn and
reordered). Each image must open with no error, hold every object whose put
returned and none whose delete returned, never return bytes that were not
put, and take an append and a second open.

Why not TLA+: the property is about byte-level states of one file under
reordering, which TLC would only see through an abstraction we would have
to trust. The enumeration runs the real code on the real bytes. It found
that the first version of the new recovery reported an interrupted delete
(dead-marking write torn across a sector, or the hole punched before the
header reached the disk) as corruption; disabling that case makes every seed
fail within a second.

Model limits: it assumes `fsync` returning means the bytes are on stable
storage (a disk that lies about flushes is out of scope), that 512-byte
sectors are written atomically, that the file exists (directory entry
durable: open syncs the directory after creating it), and that the file
system itself survives. The fs:// layout (file + rename + directory fsync)
is not modelled.

## Numbers

bench/results/2026-10-29-durability/before-after.txt:

- 65 MiB Put: before, acknowledged then every reopen failed; after,
  `ErrTooLarge`, reopen fine.
- One flipped bit in record 2 of 4: before, open succeeded and 3 objects
  vanished (overwritten by the next append); after, `ErrCorrupt` naming the
  offset and key, file unchanged.
- Power-loss model: 6 seeds, 4,721 crash images, 22 s; all pass.
- Open now reads the preallocated tail (up to 8 MiB of zeros) once: a few
  ms on local disk.

## Not done

- No salvage tool for a corrupt journal (restore from backup: item 16).
- Bit rot inside a record's header kind byte of a *live* record reads as not
  found until the next open (which then refuses).
- Errors are detected, not corrected: there is no redundancy.
- The fs:// layout and S3 are not in the power-loss model.
