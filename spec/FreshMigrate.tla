---------------------------- MODULE FreshMigrate ----------------------------
(* Offline layout migration (docs/design/01-journal-default.md).
   Names: dir (the database directory), tmp (DIR.migrate-tmp), mig
   (DIR.migrated), bak (DIR.bak-<layout>). Each name holds "none", "src"
   (the complete original), "partial" (an incomplete copy) or "copy" (a
   complete, verified, synced copy).
   fs is what the running process sees; dur is what survives a crash.
   Renames and copied data become durable only at an fsync step. A crash
   reverts fs to dur (unsynced data in tmp becomes "partial") and restarts
   the command from its resume rules. *)
EXTENDS Naturals

CONSTANT MaxCrashes

Names == {"dir", "tmp", "mig", "bak"}
VARIABLES fs, dur, pc, crashes
vars == <<fs, dur, pc, crashes>>

Init ==
  /\ fs = [n \in Names |-> IF n = "dir" THEN "src" ELSE "none"]
  /\ dur = fs
  /\ pc = "start"
  /\ crashes = 0

Rename(a, b) == fs' = [fs EXCEPT ![b] = fs[a], ![a] = "none"]

\* Resume rules: decided only by which names exist.
Start == pc = "start" /\ UNCHANGED <<fs, dur, crashes>> /\
  pc' = IF fs["mig"] # "none"
        THEN IF fs["dir"] # "none" THEN "bak" ELSE "final"
        ELSE "clean"
Clean == pc = "clean" /\ fs' = [fs EXCEPT !["tmp"] = "none"] /\ pc' = "copy" /\ UNCHANGED <<dur, crashes>>
\* Copy every key of dir into tmp, unsynced; then verify.
Copy == pc = "copy" /\ fs["dir"] = "src" /\ fs' = [fs EXCEPT !["tmp"] = "partial"] /\ pc' = "verify" /\ UNCHANGED <<dur, crashes>>
Verify == pc = "verify" /\ fs' = [fs EXCEPT !["tmp"] = "unsynced"] /\ pc' = "sync" /\ UNCHANGED <<dur, crashes>>
\* fsync data and parent directory: tmp's content and all names durable.
Sync == pc = "sync" /\ fs' = [fs EXCEPT !["tmp"] = "copy"] /\ dur' = fs' /\ pc' = "r1" /\ UNCHANGED crashes
R1 == pc = "r1" /\ fs["mig"] = "none" /\ Rename("tmp", "mig") /\ pc' = "f1" /\ UNCHANGED <<dur, crashes>>
F1 == pc = "f1" /\ dur' = fs /\ pc' = "bak" /\ UNCHANGED <<fs, crashes>>
Bak == pc = "bak" /\ fs["bak"] = "none" /\ Rename("dir", "bak") /\ pc' = "f2" /\ UNCHANGED <<dur, crashes>>
F2 == pc = "f2" /\ dur' = fs /\ pc' = "final" /\ UNCHANGED <<fs, crashes>>
Final == pc = "final" /\ fs["dir"] = "none" /\ Rename("mig", "dir") /\ pc' = "f3" /\ UNCHANGED <<dur, crashes>>
F3 == pc = "f3" /\ dur' = fs /\ pc' = "done" /\ UNCHANGED <<fs, crashes>>

Crash == pc # "done" /\ crashes < MaxCrashes /\ crashes' = crashes + 1 /\
  fs' = [n \in Names |-> IF dur[n] = "unsynced" THEN "partial" ELSE dur[n]] /\
  dur' = fs' /\ pc' = "start"

Next == Start \/ Clean \/ Copy \/ Verify \/ Sync \/ R1 \/ F1 \/ Bak \/ F2 \/ Final \/ F3 \/ Crash
Spec == Init /\ [][Next]_vars /\ WF_vars(Start \/ Clean \/ Copy \/ Verify \/ Sync \/ R1 \/ F1 \/ Bak \/ F2 \/ Final \/ F3)

TypeOK ==
  /\ \A n \in Names : fs[n] \in {"none", "src", "partial", "unsynced", "copy"}
  /\ \A n \in Names : dur[n] \in {"none", "src", "partial", "unsynced", "copy"}

\* M1: the original is always reachable, in what we see and what survives.
OriginalSafe ==
  /\ \E n \in {"dir", "bak"} : fs[n] = "src"
  /\ \E n \in {"dir", "bak"} : dur[n] = "src"
\* M1b: when dir is missing, a complete copy waits under mig.
CopyReachable ==
  /\ fs["dir"] = "none" => fs["mig"] = "copy"
  /\ dur["dir"] = "none" => dur["mig"] = "copy"
\* M2: dir and mig never name an incomplete copy.
NoPartialVisible ==
  \A n \in {"dir", "mig"} : fs[n] \in {"none", "src", "copy"} /\ dur[n] \in {"none", "src", "copy"}
\* M3: resuming always finishes with the copy in dir and the original in bak.
Finishes == <>(pc = "done")
DoneCorrect == pc = "done" => fs["dir"] = "copy" /\ fs["bak"] = "src" /\ dur = fs
=============================================================================
