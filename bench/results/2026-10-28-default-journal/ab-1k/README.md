# Default URL vs explicit journal:// at 1k msgs/s (2026-10-28, same box, back to back)

`bin/freshdb-bench msg -system <freshdb|freshdb-journal> -fsync=true -rates 1000 -producers 8 -groups 1 -duration 10s`

| run | default p99 | journal p99 | default max msg/s | journal max msg/s |
|---|---|---|---|---|
| 1 | 43.1 ms | 32.4 ms | 112,429 | 131,417 |
| 2 | 25.2 ms | 11.8 ms | 120,676 | 133,136 |

Both URLs run the same code (the default *is* the journal); the spread is
run-to-run noise. Cause of the tail on this box today: a raw
write+fdatasync probe with no freshDB at all (`diskprobe.go.txt`, 1k
syncs/s) shows 30-75 ms fdatasync stalls about once per second
(strace confirms the slow fdatasync calls at a fixed 1 s phase in the
freshDB run). On 2026-10-27 the explicit-journal msg bench got p99 3.1 ms at 1k/s
(the raw probe was not run that day, so disk state then is unknown).
A smaller, freshDB-owned stall also exists: the journal grows in 8 MiB
zero-filled chunks with a full fsync under the write lock (~40 ms once
per ~6 s at 1k x 1.3 KB/s). Not fixed yet.
