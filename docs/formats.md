# On-storage formats (v0.1.0)

> **Pre-1.0: these formats may change between minor versions.** Readers refuse
> unknown versions instead of guessing. A version bump will always come with
> a CHANGELOG entry, and a migration path once there are real users.

All integers are little-endian. "uvarint" is Go's `encoding/binary` unsigned
varint, and the decoder only accepts the minimal encoding. That makes every
payload's bytes canonical, which the hash chain relies on.

## Bucket layout

```
<prefix>/
  manifest/<id:%020d>.mf             JSON manifest, create-only, dense ids from 1
  log/<logID>/<seq:%020d>.seg        segment = one group commit (or a fence), create-only, dense seq from 1
  snap/<logID>/<lsn:%020d>.snap      optional KV+topic checkpoint (cache, never truth)
  quarantine/<logID>/<seq>-<epoch>.seg  original bytes of a repaired torn tail segment
  keys/_kek                          dev-mode key-encryption key (see SECURITY.md)
  keys/_sign                         ed25519 seed for shred certificates (dev mode)
  keys/dek/<subjectHash>             per-subject DEK, AES-GCM-wrapped by the KEK
  keys/shredded/<subjectHash>        shred marker (create-only)
  keys/certs/<subjectHash>-<lsn>.json  signed shred certificate
```

`subjectHash` is a truncated SHA-256 of the subject id (`SubjectHash` in
`keyring.go`). Raw subject ids never reach storage.

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

The manifest with the highest id is current. Creating `id+1` is how a writer
takes the lease and how branch operations commit; `ErrExists` means another
writer got there first, and this one is fenced.

## Snapshot (`.snap`)

```
u64  LSN covered
u64  len(kv)
kv lens snapshot bytes
topic lens snapshot bytes
u32  CRC32C(all of the above)
```

A snapshot that is missing or corrupt is ignored, and the lens is rebuilt from
the log.
