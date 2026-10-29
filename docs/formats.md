# On-storage formats

Describes unreleased `main`. v0.1.0 used the same segment, payload and
manifest formats and only the files layout for local directories.

> **Pre-1.0: these formats may change between minor versions.** Readers refuse
> unknown versions with `ErrVersion` instead of guessing. A version bump will
> always come with a CHANGELOG entry, and a migration path once there are
> real users. Version stamps, the upgrade/downgrade policy and the golden
> files that pin these bytes: [compatibility.md](compatibility.md).

All integers are little-endian. "uvarint" is Go's `encoding/binary` unsigned
varint, and the decoder only accepts the minimal encoding. That makes every
payload's bytes canonical, which the hash chain relies on.

## Bucket layout

```
<prefix>/
  manifest/<id:%020d>.mf             JSON manifest, create-only, dense ids from 1
  log/<logID>/<seq:%020d>.seg        segment = one group commit (or a fence), create-only, dense seq from 1
  snap/<logID>/<lsn:%020d>.snap      optional KV+topic checkpoint (cache, never truth)
  log/<logID>/<seq>.idx                 segment-index checkpoint covering segments up to <seq> (cache, never
                                        truth; create-only; deleted with the segment by GC)
  log/<logID>/<seq>.repair              repair note of a damaged segment kept by --repair-tail:
                                        "freshdb-repair v1 seq= frames= valid= size= sha256=" (create-only;
                                        the segment stays as is; deleted with the segment by GC)
  quarantine/<logID>/<seq>-<epoch>.seg  written by versions before the repair note (copy of a repaired tail)
  base/<logID>/<lsn:%020d>-e<epoch>-<nonce>.base         compaction base image (main only)
  base/<logID>/<lsn:%020d>-e<epoch>-<nonce>.<n:%06d>.val value chunk of that base, ≤ 64 MiB
  keys/_kek                          dev-mode key-encryption key (see SECURITY.md)
  keys/_sign                         ed25519 seed for shred certificates (dev mode)
  keys/dek/<subjectHash>             per-subject DEK, AES-GCM-wrapped by the KEK
  keys/shredded/<subjectHash>        shred marker (create-only)
  keys/certs/<subjectHash>-<lsn>.json  signed shred certificate
```

`subjectHash` is a truncated SHA-256 of the subject id (`SubjectHash` in
`keyring.go`). Raw subject ids never reach storage.

## Local directory layouts

A local directory stores the objects above in one of two layouts. The
objects and their bytes are the same in both.
- **files**: one file per object at its key path. This was the only
  layout in v0.1.0, and it is the default on Windows.
  - Writes go to `.tmp/`, are fsynced, renamed into place, and then the
    directory is fsynced.
- **journal** (`journal.fj`): the default for new directories on Unix.
  - 32-byte header: magic `FRESHJ1\0`, a 16-byte random salt, and the
    CRC-32C of the first 24 bytes.
  - Then records. Each record has a 12-byte header: crc u32, kind u8
    (1 put, 2 delete, 3 dead), a pad byte, klen u16 and dlen u32. The key
    and data follow.
  - The record CRC is CRC-32C over the salt, the record's own offset, the
    header, the key and the data. A dead record's CRC covers only its
    header and key.
  - A torn tail is never read as data. On open, a record failing its CRC
    is a torn append if nothing valid follows (its bytes are zeroed), an
    interrupted delete if its delete record follows (the delete is
    finished), and otherwise corruption (`store.ErrCorrupt`, file left
    unchanged). The salt keeps bytes from an older file at the same offset
    from passing the CRC.
  - Deleting an object appends a delete record. The put record is then
    marked dead and its data is hole-punched (or zeroed).
  - The file grows in 8 MiB zero-filled, fsynced chunks.
  - Writers take an exclusive `flock` and readers a shared one.

`store.DetectLayout`:
- a directory holding `journal.fj` is journal;
- any other content (except an empty `.tmp/`) makes it files;
- an empty or missing directory gets the platform default.

`freshdb migrate --db DIR [--to journal|files]` converts between the
layouts offline, with a backup. It is crash-safe; see
[operations.md](operations.md#migrating-a-v010-directory),
[design 01](design/01-journal-default.md) and
[`spec/FreshMigrate.tla`](../spec/FreshMigrate.tla).

## Value positions (in memory, not on storage)

KV values larger than `InlineMax` (default 64 B) are not kept in RAM. The
KV lens stores a position into the immutable segment that holds them
(segment, offset within the segment, length, CRC-32C of the stored bytes)
and reads them back with a ranged read. The position is derived from the
log, so this changes no on-storage bytes; it is why KV snapshots record the
inline limit (below).

## Segment (`.seg`)

```
header (32 bytes)
  0  [8]  magic "FRDBSEG1"
  8  u16  version = 1
  10 u16  reserved (0)
  12 u64  epoch     writer epoch that created this segment (fencing)
  20 u64  seq       sequence number within the log
  28 u32  CRC32C(header[0:28])
frames...
  u32  payload length (≤ 64 MiB)
  u32  CRC32C(payload)
  payload
```

A fence segment has a header and no frames.

### Frame payload (one committed transaction)

```
u64       LSN
i64       commit timestamp (unix nanos, from the DB clock; part of the log so lenses stay deterministic)
[32]      prev  = hash-chain head before this frame
uvarint   number of ops
op × n:
  u8      kind   1=PUT 2=DEL 3=APPEND 4=SHRED
  uvarint+bytes  subject   ("" = not subject-scoped; else subjectHash)
  uvarint+bytes  space     "kv", "doc:<coll>", "vec:<coll>", "__cursor", topic name for APPEND, ...
  uvarint+bytes  key
  uvarint+bytes  value     plaintext, or nonce‖AES-GCM ciphertext if subject != ""
```

**Hash chain:** `head' = SHA-256(head ‖ SHA-256(payload))`, computed over the
stored bytes (so ciphertext, for subject data). A branch's chain continues
from its parent's head at the fork LSN. Shredding deletes keys, not bytes, so
the chain still verifies after a shred.

**Encryption:** values with a subject are sealed with AES-256-GCM under the
subject's DEK. The additional data binds the ciphertext to its place:
`logID‖0‖LSN‖opIndex‖space‖0‖key`. A ciphertext copied anywhere else fails
authentication.

## Manifest (`.mf`)

```json
{
  "format": 1,
  "id": 7,
  "epoch": 4,
  "db_id": "…",
  "next_log": 3,
  "logs": {
    "L1": {"id": "L1"},
    "L2": {"id": "L2", "parent": "L1", "parent_lsn": 10, "snap_lsn": 0, "snap_key": ""}
  },
  "refs": {"main": "L2", "exp": "L1"},
  "reflog": [{"name": "main", "from": "L1", "to": "L2", "at": 10, "why": "rewind"}],
  "indexes": [{"coll": "memories", "field": "user"}]
}
```

After a compaction (`main` only) the manifest has `"format": 2`, each
compacted log has a `"base": {"lsn", "seq", "key", "chunks", "chain", "ts"}`
(the full state at `lsn`; segments before `seq` are no longer needed, nor
is the parent), and a `"garbage"` list of
`{"kind": "segs"|"snaps"|"base", "log", "below", "key", "chunks", "after"}`
entries deleted once the clock passes `after`. v0.1.0 binaries refuse format
2 instead of misreading it, so **a compacted database cannot be opened by
v0.1.0**. A base object starts with the magic `FRDBBAS1`, holds the KV image
(KV snapshot encoding, values pointing into the chunks) and the topic image,
and ends with section offsets and a CRC-32C; each value keeps its own CRC.
See [design/03-compaction.md](design/03-compaction.md).

The manifest with the highest id is current. Creating `id+1` is how a writer
takes the lease and how branch operations commit; `ErrExists` means another
writer got there first, and this one is fenced.

## Segment-index checkpoint (`.idx`)

Key `log/<log>/<seq %020d>.idx`, where `seq` is the last segment it
covers. Written with each snapshot. Version 1:

```
8B   magic "FRDBIDX1"
u8   len(log id), then the log id
u64  first segment sequence number of the log (1, or the compaction base's)
32B  hash-chain head the covered frames extend (the log's base)
u64  epoch of segment seq
i64  timestamp of the last covered frame
32B  hash-chain head after the last covered frame
u32  count (>= 1)
count x { u64 seq | u64 first LSN | u64 frame count (>= 1) }   ascending seq, contiguous LSNs
u32  CRC32C(all of the above)
```

Recovery lists the log's directory, tries the newest three checkpoints at
or above the log's first segment, and uses the first that (1) decodes,
(2) names this log, its first segment and base chain, and starts at the
LSN the log continues from, and (3) matches segment `seq` exactly: frame
count, first LSN, chain head after it, last timestamp and header epoch.
It then reads segments `seq+1` on as usual. Anything else is ignored with
a log line. Segments a checkpoint covers are not re-read at open: their
frames are checked (CRC, sequence, first LSN) when read, and `verify`
scans everything and warns about any checkpoint that differs from the
segments.

## Snapshot (`.snap`)

Key `snap/<log>/<lsn %020d>.snap`. Version 2:

```
8B   magic "FRSNAP2\n"
u64  LSN covered
32B  hash-chain head after that LSN
u8   len(log id), then the log id
u64  len(kv)
kv lens snapshot bytes
topic lens snapshot bytes
u32  CRC32C(all of the above)
```

Open uses a snapshot only if its checksum holds, its log id and LSN match
its key and the log, the LSN lies between the log's compaction floor and
its head, and the hash-chain head equals the log's own at that LSN. So the
bytes of another log's snapshot, or of a snapshot from a different history
under the same name, are never loaded. Anything else (missing, damaged,
mismatched) is ignored with a log line and the lens is rebuilt from the
log. Version 1 snapshots (the same layout without magic, LSN chain or log
id) are ignored by this build; builds before it read the magic as an LSN
past the head and ignore version 2 snapshots.

The KV part starts with the tag `KV2` and records the inline limit it was
written with. A `KV1` snapshot (v0.1.0) or one written with a different
limit is ignored the same way, and the log is replayed. Snapshots are off by
default (`WithSnapshotEvery`).
