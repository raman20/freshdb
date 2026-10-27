--------------------------- MODULE FreshBranch ---------------------------
(***************************************************************************)
(* Branches, time travel and rewind (branch.go).                          *)
(* A log is append-only and may have a parent (log, lsn). A branch is a   *)
(* ref -> log. CreateBranch(from, at) = new log with parent (ref[from],at).*)
(* Rewind(b, at) = new log forked at `at`, ref swapped, old log kept and  *)
(* reachable through the reflog. Reads at (log, lsn) = Lineage prefix.    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Branches, MaxLogs, MaxLen, Vals

VARIABLES logs, nlogs, ref, reflog
vars == <<logs, nlogs, ref, reflog>>

LogIds == 1..MaxLogs

\* full sequence of entries visible on log l up to lsn n (parent chain + own)
RECURSIVE Lineage(_, _)
Lineage(l, n) ==
  IF logs[l].plsn >= n
  THEN (IF logs[l].parent = 0 THEN <<>> ELSE Lineage(logs[l].parent, n))
  ELSE (IF logs[l].parent = 0 THEN <<>> ELSE Lineage(logs[l].parent, logs[l].plsn))
       \o SubSeq(logs[l].ents, 1, n - logs[l].plsn)
Tip(l) == logs[l].plsn + Len(logs[l].ents)

Init ==
  /\ logs = [l \in LogIds |-> [parent |-> 0, plsn |-> 0, ents |-> <<>>]]
  /\ nlogs = 1
  /\ ref = [b \in Branches |-> IF b = "main" THEN 1 ELSE 0]
  /\ reflog = {}

AppendTo(b, v) ==
  /\ ref[b] # 0 /\ Tip(ref[b]) < MaxLen
  /\ logs' = [logs EXCEPT ![ref[b]].ents = Append(@, v)]
  /\ UNCHANGED <<nlogs, ref, reflog>>

Fork(from, at) == [parent |-> from, plsn |-> at, ents |-> <<>>]

CreateBranch(b, from, at) ==
  /\ ref[b] = 0 /\ ref[from] # 0 /\ nlogs < MaxLogs /\ at <= Tip(ref[from])
  /\ logs' = [logs EXCEPT ![nlogs + 1] = Fork(ref[from], at)]
  /\ nlogs' = nlogs + 1
  /\ ref' = [ref EXCEPT ![b] = nlogs + 1]
  /\ UNCHANGED reflog

Rewind(b, at) ==
  /\ ref[b] # 0 /\ nlogs < MaxLogs /\ at <= Tip(ref[b])
  /\ logs' = [logs EXCEPT ![nlogs + 1] = Fork(ref[b], at)]
  /\ nlogs' = nlogs + 1
  /\ ref' = [ref EXCEPT ![b] = nlogs + 1]
  /\ reflog' = reflog \cup {[old |-> ref[b], oldHead |-> Tip(ref[b]),
                             new |-> nlogs + 1, at |-> at,
                             expect |-> Lineage(ref[b], at)]}

Next ==
  \/ \E b \in Branches, v \in Vals : AppendTo(b, v)
  \/ \E b, f \in Branches, at \in 0..MaxLen : CreateBranch(b, f, at) \/ Rewind(b, at)

Spec == Init /\ [][Next]_vars

\* ---- properties ----
\* time travel is stable: a read at (log, lsn) never changes, whatever
\* happens later on any branch (appends, branches, rewinds)
TimeTravelStable == [][\A l \in 1..nlogs : \A n \in 0..Tip(l) : Lineage(l, n) = Lineage(l, n)']_vars
\* after a rewind, the branch reads exactly the old prefix ...
RewindExact == \A r \in reflog : Lineage(r.new, r.at) = r.expect
\* ... and nothing is lost: the pre-rewind head is still fully readable
RewindKeepsOld == \A r \in reflog : Len(Lineage(r.old, r.oldHead)) = r.oldHead
\* lineage length equals the lsn (LSNs dense across forks)
DenseLSN == \A l \in 1..nlogs : Len(Lineage(l, Tip(l))) = Tip(l)
\* logs are append-only
AppendOnly == [][\A l \in 1..nlogs : SubSeq(logs'[l].ents, 1, Len(logs[l].ents)) = logs[l].ents
                                   /\ logs'[l].plsn = logs[l].plsn /\ logs'[l].parent = logs[l].parent]_vars
=============================================================================
