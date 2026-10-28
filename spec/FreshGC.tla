------------------------------ MODULE FreshGC ------------------------------
(* GC with retention (docs/design/08-gc-retention.md).
   Two logs: L1 (main's first log) and L2, created by a rewind of main to
   LSN P (L2's history is L1's up to P, then its own frames; the reflog
   keeps L1 reachable). Objects: segments <<"seg", l, n, 0>> (one frame
   each), bases <<"base", l, K, e>>, manifests <<"man", "-", i, 0>>.
   A process opens with a new epoch (fencing), writes frames on main's log,
   rewinds, runs GC (base at the retention cut, then one create-only
   manifest commit that may prune the reflog and drop unreachable logs),
   sweeps garbage, and deletes old manifests (keeps the newest Keep).
   Fenced (zombie) processes keep running on their old state. A create-only
   manifest commit succeeds iff its slot is free; with PostCheck the
   committer then lists manifests and gives up (deleting its stray) if a
   higher id exists. Mutants: PostCheck = FALSE, RespectDeps = FALSE,
   Keep = 0 (deletes the manifest it holds). *)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Procs, MaxLSN, MaxEpoch, MaxMan, P, W, Keep, PostCheck, RespectDeps

Logs == {"L1", "L2"}
Seg(l, n) == <<"seg", l, n, 0>>
Base(l, k, e) == <<"base", l, k, e>>
Man(i) == <<"man", "-", i, 0>>
NoMan == [e |-> 0, logs |-> {}, ref |-> "L1", reflog |-> {}, bk |-> [l \in Logs |-> 0], be |-> [l \in Logs |-> 0], garbage |-> {}]

VARIABLES objs, mdef, head, ep, cm, pc, cut, rf
vars == <<objs, mdef, head, ep, cm, pc, cut, rf>>

Ids == {i \in 1..MaxMan : Man(i) \in objs}
Latest == CHOOSE i \in Ids : \A j \in Ids : j <= i
L == mdef[Latest]
MaxEp == L.e

\* First own LSN of a log (L2's frames start after P).
Start(l) == IF l = "L2" THEN P ELSE 0
Unbased(m, l) == m.bk[l] = 0
\* Objects a manifest needs given the durable heads.
Own(m, l) == (IF m.bk[l] = 0 THEN {} ELSE {Base(l, m.bk[l], m.be[l])})
             \cup {Seg(l, n) : n \in (IF m.bk[l] = 0 THEN Start(l) ELSE m.bk[l])+1..head[l]}
Prefix(m) == IF "L2" \in m.logs /\ Unbased(m, "L2")
             THEN (IF m.bk["L1"] = 0 THEN {} ELSE {Base("L1", m.bk["L1"], m.be["L1"])})
                  \cup {Seg("L1", n) : n \in m.bk["L1"]+1..P}
             ELSE {}
Needs(m) == UNION {Own(m, l) : l \in m.logs} \cup Prefix(m)

Init ==
  /\ objs = {Man(1)}
  /\ mdef = [i \in 1..MaxMan |-> IF i = 1 THEN [NoMan EXCEPT !.logs = {"L1"}] ELSE NoMan]
  /\ head = [l \in Logs |-> 0]
  /\ ep = [p \in Procs |-> 0] /\ cm = [p \in Procs |-> 0]
  /\ pc = [p \in Procs |-> "down"]
  /\ cut = [p \in Procs |-> [l \in Logs |-> 0]]
  /\ rf = [p \in Procs |-> {}]

Open(p) ==
  /\ pc[p] = "down" /\ MaxEp < MaxEpoch /\ Latest < MaxMan
  /\ objs' = objs \cup {Man(Latest + 1)}
  /\ mdef' = [mdef EXCEPT ![Latest + 1] = [L EXCEPT !.e = MaxEp + 1]]
  /\ ep' = [ep EXCEPT ![p] = MaxEp + 1] /\ cm' = [cm EXCEPT ![p] = Latest + 1]
  /\ pc' = [pc EXCEPT ![p] = "idle"]
  /\ UNCHANGED <<head, cut, rf>>

\* A frame on main's log; a fenced writer's segment put fails (FreshLog).
Write(p) ==
  /\ pc[p] = "idle" /\ ep[p] = MaxEp
  /\ LET l == mdef[cm[p]].ref IN
       /\ head[l] < MaxLSN
       /\ head' = [head EXCEPT ![l] = @ + 1] /\ objs' = objs \cup {Seg(l, head[l] + 1)}
  /\ UNCHANGED <<mdef, ep, cm, pc, cut, rf>>

\* Create-only commit of manifest cm[p]+1 with content nm; then (PostCheck)
\* list and give up if a higher id exists.
Commit(p, nm, after) ==
  LET i == cm[p] + 1 IN
  /\ i <= MaxMan
  /\ IF Man(i) \in objs
     THEN /\ pc' = [pc EXCEPT ![p] = "fenced"] /\ UNCHANGED <<objs, mdef, cm, head>>
     ELSE IF PostCheck /\ \E j \in Ids : j > i
          THEN /\ pc' = [pc EXCEPT ![p] = "fenced"] /\ UNCHANGED <<objs, mdef, cm, head>>
               \* (create, list, see higher, delete own stray: net no change)
          ELSE /\ objs' = objs \cup {Man(i)} /\ mdef' = [mdef EXCEPT ![i] = nm]
               /\ cm' = [cm EXCEPT ![p] = i] /\ pc' = [pc EXCEPT ![p] = "idle"]
               /\ after

\* Rewind main to P: new log L2 forked from L1 at P; L1 kept by the reflog.
Rewind(p) ==
  /\ pc[p] = "idle"
  /\ LET m == mdef[cm[p]] IN
       /\ m.ref = "L1" /\ "L2" \notin m.logs /\ head["L1"] >= P /\ m.bk["L1"] <= P
       /\ head["L2"] = 0
       /\ Commit(p, [m EXCEPT !.e = ep[p], !.logs = @ \cup {"L2"}, !.ref = "L2", !.reflog = @ \cup {"L1"},
                     !.garbage = @ \cap objs],
                 head' = [head EXCEPT !["L2"] = P])
  /\ UNCHANGED <<ep, cut, rf>>

\* GC step 1: bases at the retention cut (head - W, at most the branch
\* point while L2 depends on L1); choose which reflog entries to keep.
GCBase(p) ==
  /\ pc[p] = "idle"
  /\ LET m == mdef[cm[p]]
         want(l) == IF head[l] >= W THEN head[l] - W ELSE 0
         c2 == IF "L2" \in m.logs /\ (m.bk["L2"] > 0 \/ want("L2") > P) THEN want("L2") ELSE 0
         l2dep == "L2" \in m.logs /\ m.bk["L2"] = 0 /\ c2 = 0
         c1 == IF l2dep /\ RespectDeps THEN (IF want("L1") < P THEN want("L1") ELSE P) ELSE want("L1")
         c == [l \in Logs |-> IF l \notin m.logs THEN 0
                              ELSE LET k == IF l = "L1" THEN c1 ELSE c2 IN IF k > m.bk[l] THEN k ELSE 0]
     IN /\ \E l \in Logs : c[l] > 0
        /\ cut' = [cut EXCEPT ![p] = c]
        /\ objs' = objs \cup {Base(l, c[l], ep[p]) : l \in {x \in Logs : c[x] > 0}}
        /\ \E keep \in SUBSET m.reflog : rf' = [rf EXCEPT ![p] = keep]
  /\ pc' = [pc EXCEPT ![p] = "based"]
  /\ UNCHANGED <<mdef, head, ep, cm>>

\* GC step 2: one manifest: new bases, pruned reflog, unreachable logs
\* dropped; replaced bases, covered segments and dropped logs' objects
\* become garbage.
GCCommit(p) ==
  /\ pc[p] = "based"
  /\ LET m == mdef[cm[p]]
         c == cut[p]
         bk == [l \in Logs |-> IF c[l] > 0 THEN c[l] ELSE m.bk[l]]
         be == [l \in Logs |-> IF c[l] > 0 THEN ep[p] ELSE m.be[l]]
         l2dep == "L2" \in m.logs /\ bk["L2"] = 0
         keepL1 == m.ref = "L1" \/ "L1" \in rf[p] \/ (l2dep /\ RespectDeps)
         logs == {l \in m.logs : l # "L1" \/ keepL1}
         old == {Base(l, m.bk[l], m.be[l]) : l \in {x \in m.logs : c[x] > 0 /\ m.bk[x] > 0}}
         covered == UNION {{Seg(l, n) : n \in 1..c[l]} : l \in {x \in Logs : c[x] > 0}}
         dropped == {o \in objs : o[1] # "man" /\ o[2] \in m.logs \ logs}
         nm == [e |-> ep[p], logs |-> logs, ref |-> m.ref, reflog |-> rf[p] \cap logs,
                bk |-> [l \in Logs |-> IF l \in logs THEN bk[l] ELSE 0],
                be |-> [l \in Logs |-> IF l \in logs THEN be[l] ELSE 0],
                garbage |-> (m.garbage \cap objs) \cup old \cup covered \cup dropped]
     IN Commit(p, nm, UNCHANGED head)
  /\ UNCHANGED <<ep, cut, rf>>

Sweep(p) ==
  /\ pc[p] = "idle"
  /\ \E g \in mdef[cm[p]].garbage \cap objs : objs' = objs \ {g}
  /\ UNCHANGED <<mdef, head, ep, cm, pc, cut, rf>>

\* Delete manifests more than Keep below the one we hold.
DelMan(p) ==
  /\ pc[p] = "idle"
  /\ \E i \in Ids : i + Keep <= cm[p] /\ objs' = objs \ {Man(i)}
  /\ UNCHANGED <<mdef, head, ep, cm, pc, cut, rf>>

Crash(p) ==
  /\ pc[p] # "down"
  /\ pc' = [pc EXCEPT ![p] = "down"] /\ ep' = [ep EXCEPT ![p] = 0]
  /\ UNCHANGED <<objs, mdef, head, cm, cut, rf>>

Next == \E p \in Procs : Open(p) \/ Write(p) \/ Rewind(p) \/ GCBase(p) \/ GCCommit(p)
                         \/ Sweep(p) \/ DelMan(p) \/ Crash(p)
Spec == Init /\ [][Next]_vars

TypeOK == /\ Ids # {} /\ \A p \in Procs : pc[p] \in {"down", "idle", "based", "fenced"}

\* G1: everything the latest manifest needs exists.
NoLoss == Needs(L) \subseteq objs
\* G2: every kept log keeps its retention window (base <= head - W).
Retained == \A l \in L.logs : L.bk[l] = 0 \/ L.bk[l] + W <= head[l]
\* G3: the latest manifest is never one written by a fenced epoch, and a
\* writer of the newest epoch always holds the latest manifest.
NoZombie == /\ \A i \in Ids : mdef[i].e <= MaxEp
            /\ \A p \in Procs : (pc[p] = "idle" /\ ep[p] = MaxEp) => cm[p] = Latest
\* G4: nothing listed as garbage by a present manifest is needed now.
GarbageFinal == \A i \in Ids : mdef[i].garbage \cap Needs(L) = {}
Sym == Permutations(Procs)
=============================================================================
