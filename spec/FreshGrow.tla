---- MODULE FreshGrow ----
\* Journal preallocation ahead of the writer (docs/design/09-journal-prealloc.md).
\* The file is a sequence of blocks. Writers (any process) append records at
\* `end` under the exclusive flock, after making sure the file is at least
\* end+1 blocks (extending it with zeros themselves if not). A background
\* grower extends the file with zero blocks in small steps. The question is
\* whether a zero write can land on a block that already holds a record.
\* LOCKED = TRUE: each grow step takes the flock and writes at the current
\* size (the design). LOCKED = FALSE: the grower reads the size, releases,
\* and writes later (the mutant) -- TLC finds a record overwritten.
EXTENDS Naturals
CONSTANTS Writers, Growers, MaxBlocks, LOCKED

VARIABLES file, size, end, lock, gpc, gat
vars == <<file, size, end, lock, gpc, gat>>

Blocks == 1..MaxBlocks
Procs == Writers \cup Growers

TypeOK ==
  /\ file \in [Blocks -> {"none", "zero", "rec", "lost"}]
  /\ size \in 0..MaxBlocks /\ end \in 0..MaxBlocks
  /\ lock \in Procs \cup {"free"}
  /\ gpc \in [Growers -> {"idle", "planned"}]
  /\ gat \in [Growers -> 0..MaxBlocks]

Init ==
  /\ file = [b \in Blocks |-> "none"]
  /\ size = 0 /\ end = 0 /\ lock = "free"
  /\ gpc = [g \in Growers |-> "idle"]
  /\ gat = [g \in Growers |-> 0]

\* A writer appends one record block; extends synchronously if needed.
AppendRec(w) ==
  /\ lock = "free" /\ end < MaxBlocks
  /\ file' = [file EXCEPT ![end + 1] = "rec"]
  /\ size' = IF size < end + 1 THEN end + 1 ELSE size
  /\ end' = end + 1
  /\ UNCHANGED <<lock, gpc, gat>>

ZeroAt(b) == [file EXCEPT ![b] = IF file[b] = "rec" THEN "lost" ELSE "zero"]

\* Correct grower: under the lock, write one zero block at the current size.
GrowLocked(g) ==
  /\ LOCKED /\ lock = "free" /\ size < MaxBlocks
  /\ file' = ZeroAt(size + 1)
  /\ size' = size + 1
  /\ UNCHANGED <<end, lock, gpc, gat>>

\* Mutant: read the size, then write later without holding the lock.
Plan(g) ==
  /\ ~LOCKED /\ gpc[g] = "idle" /\ size < MaxBlocks
  /\ gpc' = [gpc EXCEPT ![g] = "planned"] /\ gat' = [gat EXCEPT ![g] = size + 1]
  /\ UNCHANGED <<file, size, end, lock>>
Write(g) ==
  /\ gpc[g] = "planned"
  /\ file' = ZeroAt(gat[g])
  /\ size' = IF size < gat[g] THEN gat[g] ELSE size
  /\ gpc' = [gpc EXCEPT ![g] = "idle"]
  /\ UNCHANGED <<end, lock, gat>>

Next == \/ \E w \in Writers : AppendRec(w)
        \/ \E g \in Growers : GrowLocked(g) \/ Plan(g) \/ Write(g)

Spec == Init /\ [][Next]_vars

\* No record block is ever overwritten by preallocation.
NoLostRecord == \A b \in Blocks : file[b] # "lost"
\* Every block below `end` holds a record; the file covers `end`.
Covered == end <= size /\ \A b \in 1..end : file[b] = "rec"
====
