# Component C (topics) benchmark, fuzz and mutation results

2026-10-30, 8-core shared dev box, quiet (after the 6 h soak ended).
Before = 25a7f69, after = the component C code; the same benchmark
source compiled into both. Summary and reading: docs/components/topics.md.

- `ba.sh` → `ba_lens_*.txt`, `ba_root_*.txt`: 3 interleaved before/after
  rounds. The `BenchmarkTopicDropSubject` lines in `ba_lens_*.txt` come
  from the first version of that benchmark, which re-shredded the same
  64 subjects, so the after numbers (58–65 ns) measure no-ops. Ignore
  them; use `drop_*.txt`.
- `drop.sh` → `drop_*.txt`: the fixed DropSubject benchmark (every shred
  hits live messages, `-benchtime 64x`), 3 interleaved rounds; also
  `retain.txt`: `BenchmarkTopicRetainOpen` vs `BenchmarkTopicOpen200k`
  (after only; retention is new).
- `fuzz2.txt`: `FuzzLoadTopic`, 120 s, 18.0 M execs, PASS.
- `mutation.txt`: 16/16 hand-made mutants killed (package that killed
  each in the second column).
