---------------------------- MODULE FreshCompact ----------------------------
(* Compaction / GC of one log (docs/design/03-compaction.md).
   Objects in the store: segments <<"seg", n, 0>> (one frame each, LSN = n)
   and base images <<"base", K, e>> (state at LSN K, written by epoch e).
   Store puts and deletes are atomic; manifests are numbered densely and
   created with PutIfAbsent, modelled as "append iff my cached index is
   the latest". Processes may crash at any step and reopen with a new
   epoch; a fenced (zombie) process keeps running with its old state.
   One reader reads a manifest, then the objects it references, one by
   one, possibly while compactions and sweeps run.
   Grace: a garbage object is deleted only after every reader that started
   before it was listed has finished (the timing assumption behind
   CompactOptions.Grace). GraceGuard = FALSE / FenceCheck = FALSE are
   mutants used to show the invariants are not vacuous. *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Procs, MaxLSN, MaxEpoch, GraceGuard, FenceCheck

None == <<"none", 0, 0>>
Seg(n) == <<"seg", n, 0>>
Base(k, e) == <<"base", k, e>>

VARIABLES objs, head, mans, ep, cm, pc, kb, rm, rneed, rpc
vars == <<objs, head, mans, ep, cm, pc, kb, rm, rneed, rpc>>

Last == mans[Len(mans)]
MaxEp == Last.e
\* Objects manifest m needs, given the current durable head.
Refs(m) == (IF m.base = None THEN {} ELSE {m.base}) \cup {Seg(n) : n \in m.bk+1..head}
FirstListed(g) == CHOOSE i \in 1..Len(mans) : g \in mans[i].garbage /\ \A j \in 1..i-1 : g \notin mans[j].garbage

Init ==
  /\ objs = {}
  /\ head = 0
  /\ mans = << [e |-> 0, base |-> None, bk |-> 0, garbage |-> {}] >>
  /\ ep = [p \in Procs |-> 0]
  /\ cm = [p \in Procs |-> 0]
  /\ pc = [p \in Procs |-> "down"]
  /\ kb = [p \in Procs |-> 0]
  /\ rm = 0 /\ rneed = {} /\ rpc = "idle"

\* Open for write: take the next epoch with a new manifest (fences others).
Open(p) ==
  /\ pc[p] = "down" /\ MaxEp < MaxEpoch
  /\ mans' = Append(mans, [Last EXCEPT !.e = MaxEp + 1])
  /\ ep' = [ep EXCEPT ![p] = MaxEp + 1]
  /\ cm' = [cm EXCEPT ![p] = Len(mans) + 1]
  /\ pc' = [pc EXCEPT ![p] = "idle"]
  /\ UNCHANGED <<objs, head, kb, rm, rneed, rpc>>

\* Commit a frame. A fenced writer's segment put fails (FreshLog.tla).
Write(p) ==
  /\ pc[p] = "idle" /\ ep[p] = MaxEp /\ head < MaxLSN
  /\ head' = head + 1 /\ objs' = objs \cup {Seg(head + 1)}
  /\ UNCHANGED <<mans, ep, cm, pc, kb, rm, rneed, rpc>>

\* Step 2: write the base image at the head (commits are paused). A log
\* with no frame since its base is skipped (found by TLC: compacting it
\* again in the same epoch reuses the base key and lists the live base as
\* garbage).
WriteBase(p) ==
  /\ pc[p] = "idle" /\ head > mans[cm[p]].bk
  /\ objs' = objs \cup {Base(head, ep[p])}
  /\ kb' = [kb EXCEPT ![p] = head]
  /\ pc' = [pc EXCEPT ![p] = "based"]
  /\ UNCHANGED <<head, mans, ep, cm, rm, rneed, rpc>>

\* Step 3: one create-only manifest commit; fails if anyone committed since.
Commit(p) ==
  /\ pc[p] = "based"
  /\ pc' = [pc EXCEPT ![p] = "idle"]
  /\ IF ~FenceCheck \/ cm[p] = Len(mans)
     THEN LET m == mans[cm[p]]
              old == IF m.base = None THEN {} ELSE {m.base}
          IN /\ mans' = Append(mans, [e |-> ep[p], base |-> Base(kb[p], ep[p]), bk |-> kb[p],
                         garbage |-> (m.garbage \cap objs) \cup old \cup {Seg(n) : n \in m.bk+1..kb[p]}])
             /\ cm' = [cm EXCEPT ![p] = Len(mans) + 1]
     ELSE UNCHANGED <<mans, cm>>
  /\ UNCHANGED <<objs, head, ep, kb, rm, rneed, rpc>>

\* Step 5a: delete garbage listed in the manifest p knows, after the grace.
Sweep(p) ==
  /\ pc[p] = "idle"
  /\ \E g \in mans[cm[p]].garbage \cap objs :
       /\ GraceGuard => ~(rpc = "reading" /\ rm < FirstListed(g))
       /\ objs' = objs \ {g}
  /\ UNCHANGED <<head, mans, ep, cm, pc, kb, rm, rneed, rpc>>

\* Step 5b: delete a base of an older epoch that the latest manifest does
\* not reference (left by a crashed or fenced compaction).
SweepOrphan(p) ==
  /\ pc[p] = "idle"
  /\ \E o \in objs :
       /\ o[1] = "base" /\ o[3] < ep[p] /\ o # Last.base
       /\ GraceGuard => ~(rpc = "reading" /\ o \in rneed)
       /\ objs' = objs \ {o}
  /\ UNCHANGED <<head, mans, ep, cm, pc, kb, rm, rneed, rpc>>

Crash(p) ==
  /\ pc[p] # "down"
  /\ pc' = [pc EXCEPT ![p] = "down"] /\ ep' = [ep EXCEPT ![p] = 0]
  /\ UNCHANGED <<objs, head, mans, cm, kb, rm, rneed, rpc>>

RStart ==
  /\ rpc \in {"idle", "done"} /\ rm < Len(mans)
  /\ rm' = Len(mans) /\ rneed' = Refs(Last) /\ rpc' = "reading"
  /\ UNCHANGED <<objs, head, mans, ep, cm, pc, kb>>

RRead ==
  /\ rpc = "reading"
  /\ IF rneed = {} THEN rpc' = "done" /\ UNCHANGED rneed
     ELSE \E o \in rneed :
            IF o \in objs THEN rneed' = rneed \ {o} /\ UNCHANGED rpc
            ELSE rpc' = "failed" /\ UNCHANGED rneed
  /\ UNCHANGED <<objs, head, mans, ep, cm, pc, kb, rm>>

Next == \E p \in Procs : Open(p) \/ Write(p) \/ WriteBase(p) \/ Commit(p) \/ Sweep(p) \/ SweepOrphan(p) \/ Crash(p)
        \/ RStart \/ RRead
Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ head \in 0..MaxLSN
  /\ \A p \in Procs : pc[p] \in {"down", "idle", "based"}
  /\ rpc \in {"idle", "reading", "done", "failed"}

\* C1: everything the latest manifest needs exists: no committed frame lost.
NoLoss == Refs(Last) \subseteq objs /\ Last.bk <= head
\* C2: fencing: manifest epochs never go backwards (no zombie commit).
EpochMonotone == \A i \in 1..Len(mans)-1 : mans[i].e <= mans[i+1].e
\* C3: garbage is final: never referenced by the same or a later manifest.
GarbageFinal == \A i \in 1..Len(mans) : \A j \in i..Len(mans) : mans[i].garbage \cap Refs(mans[j]) = {}
\* C4: under the grace assumption a reader never misses an object.
ReaderOK == rpc # "failed"
=============================================================================
