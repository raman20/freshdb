# 18. Soak and chaos (item 18; gap S3.7)

## Problem

Crash tests and the simulator ran for seconds to minutes per CI run. No
run exercised hours of real time, a real server binary killed with
SIGKILL under load, or compaction, GC and backups overlapping writes.

## Model

No new formal model. The checker encodes the same safety properties the
TLA+ model and simulator state (designs 01, 12):

- a key's value is one written between the last acknowledged and the last
  attempted operation on it (no lost ack, no value from nowhere, no
  resurrected older value);
- a transaction's keys are seen together;
- a topic holds every acked message, in order, no duplicates or phantoms;
- a restored backup satisfies the same bounds as of the backup.

Unknown outcomes (timeouts, connection reset by kill -9) widen the
allowed window instead of being errors.

## Built

- `bench/cmd/freshdb-soak`: 8 client goroutines (puts, deletes, large
  values, multi-key transactions, publishes) against `freshdb serve`;
  kill -9 every 40–120 s and restart; compaction every 20 s, GC every
  61 s, online backup every 90 s, each backup restored and checked;
  full checks after every restart; status line every 30 s with RSS,
  disk and commit p99.
- Fix found while building it: the per-key model kept every attempted
  value, so the harness itself was OOM-killed (13 GB). Values
  below the acked index are now pruned, except while a backup check pins
  the backup's lower bounds. Harness RSS stays in the tens of MB.
- Simulator: `-steps` flag / `FRESHDB_SIM_STEPS` and `FRESHDB_SIM_FIRST`
  so long runs can be sharded across processes.
- Simulator finding: at 2 000 steps, storm seeds (read-fault rate 1.5–3 %
  per GET) failed with "could not open after 200 attempts". Open re-reads
  every object, so it succeeds with probability ~(1-p)^objects; this is a
  liveness limit of the harness's fault model (real clients retry each
  request; see docs/s3.md), not a safety violation. After 100 failed opens
  the simulator now lets the storm pass and counts it (`calm opens`).
  Open's cost growing with objects since the last compaction is S2.6.

## Results

See `bench/results/2026-10-29-soak/`.

## Open

- One machine, one filesystem (ext4 on a VM disk), loopback HTTP; no
  network partitions between client and server, no S3 backend in the soak.
- The simulator has a logical clock only; its runs are reported as seeds ×
  steps and event counts, not as equivalent wall time.
- No soak longer than the run recorded here; no soak in CI (the release
  checklist requires ≥ 1 h before a release).
