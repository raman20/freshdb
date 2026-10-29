# Component C: topics and consumer groups

Review of topics and consumer groups, completed 2026-10-30 (IST). Code:
`lens/topic.go` (the topic lens), the `TopicTx` and `Group` API in
`tx.go`, topic retention in `compact.go` (`TopicRetention`,
`topicCuts`, `writeBase`), the HTTP handlers in `server/server.go`, and
the CLI flags in `cmd/freshdb`. Commits: 7c9db6d (code), this document
and the benchmark files (docs). This review does not claim topics are
production ready; "Verdict" says what it does claim.

## Contract

1. **Offsets.** A topic is an append-only sequence. The n-th message
   committed to a topic gets offset n (from 0), assigned at commit in
   log order. Offsets never change, including after retention.
   `Len()` is the next offset (every message ever committed, retained
   or not); `Start()` is the first offset still stored (0 unless
   retention removed older messages).
2. **Reads.** `Read(from, max)` returns up to `max` committed messages
   with offset ≥ `max(from, Start)`, as of the transaction's snapshot.
   A read from below `Start` silently starts at `Start`. Values are
   copies; a shredded subject's message reads as `Redacted` with no
   value.
3. **Consumer groups.** A group's cursor is a KV key
   (`group \x00 topic` in the cursor space), 8 bytes little-endian, the
   next offset to consume; no key means 0. `Fetch` reads the cursor
   (so it is in the transaction's OCC read set) and then the messages.
   `Ack(m)` moves the cursor to `m.Offset+1` and **never back**; `Seek`
   moves it anywhere. A group name containing NUL, or acking a message
   of another topic, is `ErrBadName`; a cursor that is not 8 bytes is
   `ErrCorrupt`.
4. **Delivery semantics.**
   - `Fetch` + processing that writes only to this DB + `Ack`, in one
     `Update`: **exactly-once**. Two transactions that fetched the same
     cursor cannot both commit (OCC on the cursor key); a crash before
     commit leaves the cursor where it was.
   - Side effects outside the DB inside the transaction (an HTTP call,
     an email): **at-least-once**. A conflict or crash after the effect
     and before commit redelivers the message.
   - HTTP `POST /v1/topics/{t}/groups/{g}/next`: fetch and ack in one
     server-side transaction, then respond: **at-most-once** to the
     client (a lost response loses the messages for that group).
     Exactly-once needs the embedded API (Fetch/Ack in your own
     `Update`); plain `GET` reads with a client-kept offset give
     at-least-once.
5. **Strict loading.** `LoadTopic` (snapshots and compaction bases)
   accepts only bytes `Snapshot()` can write; anything else is
   `ErrBadSnapshot` (snapshot ignored and the log replayed; a base is
   `ErrCorrupt`). Loading never panics.
6. **Shredding.** `DropSubject(s)` redacts every stored message of
   subject `s` before it returns.
7. **Retention** (new, off by default): see "Retention design".
8. **Bounded HTTP reads.** `GET /v1/topics/{t}` and `.../next` return at
   most 1000 messages per request. For `GET`, `max` 0 or absent means
   1000; for `next`, absent means 10 (unchanged) and 0 means 1000.
9. **Concurrency.** `Apply`, `DropSubject` and `SetAll` run under the
   view's write lock; `Read`, `Len`, `Start`, `Snapshot` under its read
   lock. The lens is pure (no goroutines, no sync; lens import rule).

## Defects found and fixed

- **C-D1, lax decoder.** `LoadTopic` accepted topics out of order or
  repeated (a later copy silently replaced an earlier one), LSNs going
  back, a message count up to the remaining byte length (each message
  needs at least 33 bytes, so a short input allocated ~70× its size in
  `Msg` structs before failing), and a redacted message with a value or
  without a subject. A damaged base could
  thus load with wrong offsets instead of being refused. Found in the
  line-by-line read; `TestLoadTopicRejectsMalformed` covers each case,
  and `FuzzLoadTopic` checks that any accepted input re-encodes to the
  same bytes.
- **C-D2, group cursor bugs.**
  - `Ack` of an earlier message after a later one moved the cursor
    back, so already processed messages were redelivered. It now never
    moves back (`TestGroupAckNeverMovesBack`).
  - `Ack` of a message from another topic moved this group's cursor to
    that message's offset (`TestGroupAckOtherTopic`).
  - A group name with a NUL byte could address another group's cursor
    (`"a\x00b"` on topic `c` and `"a"` on topic `"b\x00c"` share a key);
    now `ErrBadName`, HTTP 400 (`TestGroupNameNUL`).
  - A cursor value that was not 8 bytes read as 0, so the group
    restarted from the beginning; now `ErrCorrupt`
    (`TestGroupCursorCorrupt`).
  - Hardening: `openMsg` now also matches the topic space when it finds
    a subject message's log entry (it matched by sealed bytes only).
- **C-D3, unbounded HTTP reads.** `GET /v1/topics/{t}` without `max`
  returned the whole topic in one response, and `next?max=0` fetched
  and acked the whole topic. Both are capped at 1000
  (`TestTopicReadCapped`). This changes behaviour for clients that
  relied on "no max = everything"; they now page with `from`. Our own
  soak driver did (`max=100000`) and would have reported false
  failures against the new server once a topic passed 1000 messages; it
  now pages. Checked with a 3-minute soak against the new server (SOAK
  OK: 5 kill -9 restarts, 6 full checks; topic t0 held more than 1000
  messages, so the pager was exercised).
- **C-D4, `DropSubject` was O(all messages)** on every shred and on
  every open that replays one. The lens keeps a per-subject index of
  unredacted messages; a shred touches only that subject's messages
  (3.0–3.2 ms → 74–138 µs for 1,024 messages out of 1M, below).

Research/ROADMAP gap closed: **topic retention** did not exist; every
message ever published stayed in RAM and in every base.

## Retention design

- `TopicRetention{MaxAge, MaxMessages, IgnoreGroups}` on
  `CompactOptions.Topics` and `GCOptions.Topics`; CLI
  `--topic-max-age`, `--topic-max-messages`, `--topic-ignore-groups`
  on `compact`, `gc` and `serve` (the background compactor);
  `topic-max-age`, `topic-max-messages`, `topic-ignore-groups` query
  parameters on `/v1/compact` and `/v1/gc` (bad values: 400).
- For each topic, the cut is the highest offset both limits allow:
  keep the newest `MaxMessages`, and drop the oldest messages up to the
  first one whose commit time is not older than `MaxAge`. The clock is
  read only when `MaxAge` is set.
- Unless `IgnoreGroups`, the cut is lowered to the smallest committed
  cursor of any group of that topic (a scan of the cursor space at the
  compaction head; an unreadable cursor counts as 0, which keeps
  everything).
- The base is written from the cut: messages below it are not copied,
  so they leave storage with the replaced objects and leave RAM when the
  branch reloads from the base. Offsets are unchanged: the base records
  each trimmed topic's start. `CompactStats.Trimmed` counts the dropped
  messages.
- Encoding: `TOPIC1` as before when no topic is trimmed (so untrimmed
  snapshots, bases and digests are byte-identical to before); `TOPIC2`
  (a start offset per topic) only when some start is non-zero. Binaries
  from before this review refuse a `TOPIC2` base as corrupt rather than
  misreading it; this matters only after retention is used.
- `Compact` gives a log whose base is already at its head a new base
  only when retention would cut something there. `GC` applies retention
  only to bases it writes anyway; use `Compact` to trim.

## TLA+ alignment

- **FreshTxn** (existing) models consume + KV write + ack in one OCC
  transaction with 3 consumers and a crash: `ExactlyOnce` holds, and
  removing OCC validation violates it. The code matches the model's
  assumption that the cursor is read inside the transaction: `Fetch`
  goes through `tx.get`, which records the read.
- **FreshRetain** (new, `spec/FreshRetain.tla`): one topic, 2 groups ×
  2 workers each running Fetch (with the clamp-to-start read) → Commit
  (OCC on the cursor) or Abort, publishing, and compaction moving the
  start anywhere up to the end, clamped to the minimum cursor when
  `Clamp`. Invariants `TypeOK`, `ExactlyOnce` (each group processed
  every offset below its cursor exactly once, none above), `NoLoss`.
  - `FreshRetain.cfg` (MaxMsgs 4, Clamp TRUE): 73,958 states, 9,143
    distinct, no error; in `spec/run_tlc.sh`, so CI runs it.
  - `FreshRetain_noclamp.cfg` (`IgnoreGroups`): `ExactlyOnce` violated,
    as documented: a lagging group skips messages.
  - Mutant with the OCC check removed: `ExactlyOnce` violated (needed 2
    workers per group; the first 1-worker model could not see it).
- **Where code and model differ.** The model knows every group from the
  start (cursor 0). The code only sees groups that have committed a
  cursor: a group that has never acked does not hold retention back and
  starts at the cut. The model has one branch; the code clamps only to
  cursors on the branch being compacted. Writers are paused in the model
  during compaction; in the code compaction cuts at a head read under
  the view lock and later commits are kept in full.

## Tests

| | before (25a7f69) | after |
|---|---:|---:|
| topic / group test functions (root, lens, server) | 1 | 14 |
| fuzzers on topic decoders | 0 | 1 (`FuzzLoadTopic`) |
| `lens/topic.go` statement coverage (lens tests) | 76.1 % | 100 % |

- `FuzzLoadTopic`: any accepted input must re-encode to identical bytes,
  satisfy the offset invariant (start + index), and survive
  Read/Len/Start/DropSubject/Digest. 120 s, 18.0 M execs, clean (seeds
  include TOPIC1, TOPIC2 and the malformed cases).
- `TestLoadTopicRejectsMalformed`: TOPIC2 with every start 0, start +
  count overflow, an unknown tag (`TOPIC3`), plus the C-D1 cases.
- `TestTopicRefinesModel` (rapid): appends, shreds, trims via `SetAll`,
  snapshot reloads; after each step every Read/Len/Start/Snapshot
  equals a slice model.
- Root: `TestTopicRetentionMaxMessages`, `…Groups` (clamp and
  `IgnoreGroups`), `…MaxAge` (via GC with a test clock), `…Subjects`
  (a trimmed base keeps shred working), the four C-D2 tests above,
  `TestTopicsAndExactlyOnceConsumer`.
- Server: `TestTopicReadCapped`, `TestCompactTopicRetention` (bad
  parameters → 400; compact keeps start 6 of 10; gc on an already-based
  log changes nothing).
- **Simulator:** half of the non-GC compactions use
  `Topics.MaxMessages = rng.Intn(8)`. The checker requires `Len` = the
  model's length, start ≤ length, start never decreasing, start > 0
  only after retention, and the stored messages = model[start:]. 300
  seeds × 60 steps: 40 trims, OK; the full suite's 2,000-seed run
  passes.
- Race: the full suite runs `-race` over root and server, including
  concurrent consumers.
- Crash points: the lens holds no durable state; retention is part of
  the base write, whose crash/fencing behaviour is the existing
  compaction model (FreshCompact) and crash tests.
- **Mutation:** 16 hand-made mutants (clamp to min cursor, MaxMessages
  off by one, MaxAge boundary, min over groups, retention without new
  commits, base keeps start, Read clamp, Apply offset, TOPIC2 needs a
  start, TOPIC2 encoding, DropSubject via the index, LSN order,
  start+count overflow, duplicate spaces, Ack never back, cursor
  length): **16/16 killed**.
- **Coverage, uncovered lines (root, `-coverpkg=./`):** `topicCuts`
  97.1 %; the one uncovered statement is the `continue` for a cursor key
  without a NUL, which the API cannot write (`Group.key` always adds
  one). `Group.Cursor` and `Ack` miss only the branch where the KV read
  itself fails (a store error), covered generically by the read-fault
  tests of component A.

Raw output: bench/results/2026-10-30-topics (fuzz2.txt, mutation.txt).

## Benchmarks

Before = 25a7f69, after = the final code, same benchmark source compiled
into both, run interleaved 3× on a quiet 8-core box after the 6 h soak
ended (2026-10-30 ~02:20 IST). Lens: 1M messages of 256 B in 16
topics, 1/16 of them with a subject (64 subjects). Root: mem store.

| benchmark | before | after |
|---|---|---|
| lens Apply (ns/msg) | 380–397 | 366–405 |
| lens heap after Apply (B/msg) | 338.6 | 341.3 (+0.8 %, subject index) |
| lens snapshot load | 1,044–1,196 MB/s | 903–1,087 MB/s |
| lens heap after snapshot load (B/msg) | 329.0 | 330.7 |
| lens Read 100 | 2.21–2.38 µs | 2.18–2.42 µs |
| lens DropSubject, live subject, 1,024 of 1M msgs (`-benchtime 64x`) | 2.99–3.23 ms | 74–138 µs |
| Publish, 1 msg/txn | 5.9–6.5 µs | 6.1–6.8 µs |
| Fetch 100 + Ack in one txn | 40–53 µs | 31–47 µs |
| Open, 200k msgs, replay | 114–121 ms, 338.5 B/msg | 121–140 ms, 338.5 B/msg |
| Compact with MaxMessages 1000, 200k msgs (new) | — | 19–22 ms |
| heap after reopen, per published msg (new) | 338.5 (no retention) | 1.79 |

Read plainly:

- **Snapshot load is ~10–15 % slower in the median** (stricter decode
  and building the subject index). Opening 200k messages by replay was
  ~10 % slower in the median in this run; the lens Apply numbers show no
  matching change, so this is likely noise, but it is recorded.
- The first DropSubject benchmark shredded the same 64 subjects over
  and over, so after the first 64 calls the indexed version did nothing
  (58–65 ns in `ba_lens_after.txt`). That number is meaningless and is
  not quoted; the benchmark now reloads every 64 shreds so each one
  hits live messages (`drop_*.txt`).
- Retention is what bounds topic memory: without it RAM grows ~339 B per
  256 B message kept, forever.

Raw output: bench/results/2026-10-30-topics.

## Residual risks

- **All kept messages live in RAM** (~339 B per 256 B message) and a
  base stores them inline in one object. Without retention a busy topic
  grows without bound. Retention runs only at compaction.
- **A group that never committed does not hold retention**, and groups
  on other branches are not considered: a branch created later starts
  at the cut.
- **A `Seek` or `Read` below `Start` silently starts at `Start`**; the
  caller can detect the gap only by comparing the first offset (or
  `Start()`).
- `Start()` reports the view's current start, not the start as of the
  transaction's snapshot; it can only be ahead of what the snapshot saw.
- `MaxAge` uses commit timestamps from the writer's clock; a clock that
  jumps back keeps messages longer (the age cut stops at the first
  young message), never drops newer ones.
- HTTP `next` is at-most-once (contract 4); this is by design, not
  fixed.
- Server RSS and disk growth seen in the 6 h soak (component A notes)
  are not root-caused; topic memory is one candidate since the soak
  publishes without retention.

## Verdict

Topics and consumer groups meet the contract above under the tested
model: the fuzzer, the rapid model, the simulator with retention and
TLC (FreshTxn, FreshRetain) found nothing open after C-D1 to C-D4,
each fixed with a regression test. Retention is new and off by
default. The component is **done for this review**. This is not a claim
of production readiness; the residual risks above stand.
