# 17. S3 correctness (item 20; gaps S5.4, S6.5)

## Problem

S3 support was tested with MinIO in CI on the happy path only: no
injected 5xx, resets or lost responses; no request counts; no way for an
operator to check that an endpoint has the semantics fencing depends on.

## Model

The fencing argument (designs 01, 12 and `tla/`) assumes one primitive:
an atomic create-only PUT. LIST and GET may be stale without breaking
safety: a writer that starts from an old manifest fails its create of the
next id and is fenced. The TLA+ model already lets the environment lose a
response after the store applied it; the engine handles that in
`putExactlyOnce` (on 412, read back; byte-identical = our earlier
attempt). No new formal model: the new work tests that the implementation
matches the existing one under real HTTP faults.

## Built

- `store.S3Check` / `freshdb s3check URL`: read-after-write, create-only,
  concurrent creates (10 × 8 writers, one winner), LIST-after-write,
  ranged GET, not-found/delete visibility, large single PUT, under a
  random scratch prefix it removes. Unit test: a store whose create
  silently overwrites fails it (`store/s3check_test.go`).
- `TestS3Faults`: a reverse proxy (keeps the signed Host header) injects
  503 SlowDown, resets before forwarding, and lost responses after the
  request was applied. Single writer; every acked write is checked
  directly against MinIO afterwards, then Verify.
- `TestS3Cost`: request counts per operation through the same proxy.

## Results (MinIO, loopback, 2026-10-29)

- 15% faults / 300 commits and 60% / 60 commits: 0 false fences, no
  unknown outcomes reached the caller (minio-go retries plus the 412
  read-back absorbed them), every acked write present, Verify OK.
- Cost: 1 PUT per serial commit, 0.06 with 32 concurrent writers, ~2 GETs
  per segment at open (807 for ~400 segments). Table in docs/s3.md.

## Open

- AWS S3, R2, GCS untested (no credentials); `s3check` lets users test.
- Snapshots are one object; above 5 GiB a single PUT fails on AWS (S5.5).
- Open's GET count grows with segments since the last compaction (S2.6);
  no automatic compaction trigger.
