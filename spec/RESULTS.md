# TLA+ model-checking results

TLC 2.19 (tla2tools v1.8.0), Java 21, 8 workers, on the dev box (2026-10-27).
Run with `spec/run_tlc.sh`. Every run below explored the **complete** bounded
state space (0 states left on queue). Bounds are in each `.cfg`.

| Spec | What it models | States generated | Distinct | Depth | Result |
|---|---|---:|---:|---:|---|
| FreshLog | object-store log: create-only manifests + epochs, recovery scan, fence segment, group commit, lost/delayed/duplicated PUTs, give-up (unknown outcome), crash, power-loss torn tail + repair, 2 racing writers | 10,415,103 | 2,475,220 | 32 | no error |
| FreshTxn | OCC serializable txns; consume-message + write-KV + ack-cursor in one txn; 3 concurrent consumers + blind writer; durable-prefix crash | 21,685,744 | 2,421,454 | 20 | no error |
| FreshBranch | append-only logs with parents; create branch, rewind (fork + ref swap + reflog), time travel | 3,430,206 | 1,845,287 | 16 | no error |
| FreshShred | DEK per subject, marker → delete → audit entry + certificate; crash between steps; DEK create landing late; sweep; hash chain | 25,570 | 6,481 | 16 | no error (incl. liveness) |

## Properties checked

- **FreshLog** (invariants): `Durability` (acked ⊆ recovered log), `NoHoles`,
  `EpochMonotone` (no split brain: an older writer can't append after a newer
  fence), `NoDoubleApply`, `NoCorruption` (recovery never meets damage below the
  tail), `TornOnlyUnacked`, `SingleLeader`, `ManifestMonotone`.
  Temporal: `AckedStable`, `Immutable` (segments never change except torn-tail repair).
- **FreshTxn**: `ExactlyOnce` (each message processed ≤ once), `AtomicCursor`
  (processed set = {0..cursor-1}, i.e. consume+KV+ack are atomic), `Serializable`
  (every committed read equals the serial-order state before it), `DurablePrefix`.
- **FreshBranch**: `RewindExact`, `RewindKeepsOld` (pre-rewind head still readable),
  `DenseLSN`; temporal `AppendOnly`, `TimeTravelStable` (no step ever changes the
  contents of any (log, lsn) read).
- **FreshShred**: `ShreddedNeverRevealed`, `NoWritesAfterShred`, `ChainVerifiable`,
  `CertsVerify`; temporal `AppendOnly`, `KeyEventuallyGone` (under weak fairness
  of sweep/land: `<>[](shredded ⇒ no DEK)`).

## Bugs found by the models (and fixed)

1. **Real design + code bug — FreshLog, `EpochMonotone` violated.** A slow opener
   (epoch 1) that created its manifest before a newer opener (epoch 2), but
   scanned the log *after* epoch 2 wrote its fence, appended its own fence with
   the older epoch. The next recovery would then refuse the log as corrupt
   ("epoch went backwards"), bricking the database. Counterexample: 9 steps.
   Fix: recovery fails with `ErrFenced` and writes nothing if it sees any epoch
   newer than its own (`log.go recoverLog`, also before torn-tail repair; spec
   `Superseded`). Regression test `TestConcurrentOpenersNeverBrickLog` **fails
   without the fix and passes with it**. The deterministic simulator had not
   caught this because it never interleaves two concurrent `Open`s.
2. **Spec bug — FreshBranch, `AppendOnly` violated** at `Rewind("main",0)`: the
   property quantified over unallocated log slots, so initialising a fresh log
   looked like mutating one. Fixed by quantifying over `1..nlogs`. The code
   was not affected (no existing log's parent ever changes).
3. The shred spec models a race the **simulator** found first (seed 769): a
   DEK-create PUT that timed out but was still in flight can land after the
   shred deleted the DEK. Reads stay blocked by the shred marker
   (`ShreddedNeverRevealed`), and the sweep at open/Verify gives
   `KeyEventuallyGone`.

## Mutation checks (are the properties strong enough?)

| Mutation | Result |
|---|---|
| FreshLog without the `Superseded` check | `EpochMonotone` violated (bug #1 above) |
| FreshTxn with OCC validation removed | `ExactlyOnce` violated (911 states) |
| FreshShred with `Sweep` disabled | `KeyEventuallyGone` violated |
| FreshShred with reads ignoring the shred marker | `ShreddedNeverRevealed` violated |

## Limits (honest)

- These are bounded models (2 writers, 3 txns, 5 segments, etc.). Bounded
  model checking finds small counterexamples well; it is not a proof for all sizes.
- The specs are written by hand to match the Go code. Nothing mechanically
  checks that the code matches them. The deterministic simulator and property
  tests (see README) check the code itself against reference models.
- Torn writes are over-approximated as any subset of a batch's frames (the
  real format keeps a prefix).
- Snapshots, vector/doc lenses and the HTTP layer are not modelled.
