---------------------------- MODULE FreshLog ----------------------------
(***************************************************************************)
(* freshDB log protocol on an object store with create-only PUT.             *)
(*                                                                         *)
(* Models (matching log.go / db.go / writer.go / manifest.go):             *)
(*  - manifests: dense ids, create-only; Open = read latest id m, then     *)
(*    create m+1 with epoch(latest)+1 (lost race => retry)                 *)
(*  - recovery: list the dense prefix of segments, then write a header-    *)
(*    only FENCE segment at the next seq (create-only; lost race => rescan)*)
(*  - group commit: one segment per batch, create-only PUT at next seq;    *)
(*    ack ONLY after reading back that our exact bytes are stored          *)
(*  - network: requests can be lost, delayed past a crash, duplicated;     *)
(*    a writer may give up (ErrUnknownOutcome) while its PUT is in flight  *)
(*  - power loss: a crashed writer's in-flight PUT may land TORN (a subset *)
(*    of its frames: CRC framing keeps a prefix; subset over-approximates) *)
(*    Recovery repairs a torn TAIL in place; a torn non-tail = corruption  *)
(*  - two writer processes racing (zombie + new opener)                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Writers, Txns, MaxSeq, MaxEpoch, MaxBatch

NoSeg == [epoch |-> 0, batch |-> {}, fence |-> FALSE, torn |-> FALSE]
Rec   == [epoch : 1..MaxEpoch, batch : SUBSET Txns, fence : BOOLEAN, torn : BOOLEAN]
Seqs  == 1..MaxSeq
Modes == {"down", "acq", "scan", "fence", "open", "wait", "dead"}

VARIABLES
  segs,      \* object store: seq -> segment record (or NoSeg)
  mans,      \* object store: manifest id -> epoch (a sequence, ids dense)
  inflight,  \* PUT requests on the wire (may land any time, even after a crash)
  ws,        \* per-writer volatile state
  acked,     \* txns acknowledged to clients
  used,      \* txns ever submitted (clients never resubmit an ambiguous txn)
  corrupt    \* recovery found damage it must refuse (ErrCorrupt)

vars == <<segs, mans, inflight, ws, acked, used, corrupt>>

Present(s) == segs[s] # NoSeg
DenseLen == CHOOSE n \in 0..MaxSeq :
              (\A s \in 1..n : Present(s)) /\ (n = MaxSeq \/ ~Present(n+1))
Committed == UNION { segs[s].batch : s \in {t \in Seqs : t <= DenseLen} }
LatestEpoch == IF Len(mans) = 0 THEN 0 ELSE mans[Len(mans)]

WS == [mode : Modes, epoch : 0..MaxEpoch, pend : 0..MaxEpoch+1, next : 0..MaxSeq+1,
       out : [seq : 0..MaxSeq, rec : Rec \cup {NoSeg}]]
Idle == [mode |-> "down", epoch |-> 0, pend |-> 0, next |-> 0,
         out |-> [seq |-> 0, rec |-> NoSeg]]

TypeOK ==
  /\ segs \in [Seqs -> Rec \cup {NoSeg}]
  /\ ws \in [Writers -> WS]
  /\ acked \subseteq Txns /\ used \subseteq Txns
  /\ corrupt \in BOOLEAN

Init ==
  /\ segs = [s \in Seqs |-> NoSeg]
  /\ mans = <<>>
  /\ inflight = {}
  /\ ws = [w \in Writers |-> Idle]
  /\ acked = {} /\ used = {}
  /\ corrupt = FALSE

\* ---- open: acquire the write lease by creating manifest m+1 ----
ReadManifest(w) ==
  /\ ws[w].mode = "down"
  /\ ws' = [ws EXCEPT ![w] = [Idle EXCEPT !.mode = "acq", !.pend = Len(mans)]]
  /\ UNCHANGED <<segs, mans, inflight, acked, used, corrupt>>

PutManifest(w) ==
  /\ ws[w].mode = "acq"
  /\ IF Len(mans) = ws[w].pend           \* create-only PUT of id pend+1
     THEN /\ LatestEpoch + 1 <= MaxEpoch
          /\ mans' = Append(mans, LatestEpoch + 1)
          /\ ws' = [ws EXCEPT ![w].mode = "scan", ![w].epoch = LatestEpoch + 1]
     ELSE /\ mans' = mans                \* 412: someone else won; retry
          /\ ws' = [ws EXCEPT ![w] = Idle]
  /\ UNCHANGED <<segs, inflight, acked, used, corrupt>>

\* ---- recovery: scan dense prefix, repair torn tail, then fence ----
Superseded(w, n) == \E s \in 1..n : segs[s].epoch > ws[w].epoch

\* Fix found by TLC: without the Superseded check a slow opener (older epoch)
\* could write its fence after a newer writer's fence => EpochMonotone fails
\* and the next recovery refuses the log. Code: log.go recoverLog (ErrFenced).
Scan(w) ==
  /\ ws[w].mode = "scan"
  /\ LET n == DenseLen IN
     /\ n < MaxSeq
     /\ IF Superseded(w, n)
        THEN /\ ws' = [ws EXCEPT ![w].mode = "dead"]       \* ErrFenced
             /\ UNCHANGED <<segs, corrupt>>
        ELSE IF \E s \in 1..n : segs[s].torn /\ s < n
        THEN /\ corrupt' = TRUE                             \* damage below the tail
             /\ ws' = [ws EXCEPT ![w].mode = "dead"]
             /\ segs' = segs
        ELSE /\ corrupt' = corrupt
             /\ segs' = IF n > 0 /\ segs[n].torn          \* repair torn tail
                        THEN [segs EXCEPT ![n].torn = FALSE]
                        ELSE segs
             /\ ws' = [ws EXCEPT ![w].mode = "fence", ![w].next = n + 1]
  /\ UNCHANGED <<mans, inflight, acked, used>>

Fence(w) ==
  /\ ws[w].mode = "fence"
  /\ LET s == ws[w].next IN
     IF ~Present(s)
     THEN /\ segs' = [segs EXCEPT ![s] = [epoch |-> ws[w].epoch, batch |-> {},
                                          fence |-> TRUE, torn |-> FALSE]]
          /\ ws' = [ws EXCEPT ![w].mode = "open", ![w].next = s + 1]
     ELSE /\ segs' = segs                               \* lost race: rescan
          /\ ws' = [ws EXCEPT ![w].mode = "scan"]
  /\ UNCHANGED <<mans, inflight, acked, used, corrupt>>

\* ---- group commit ----
Send(w) ==
  /\ ws[w].mode = "open"
  /\ ws[w].next <= MaxSeq
  /\ \E B \in SUBSET (Txns \ used) :
       /\ B # {} /\ Cardinality(B) <= MaxBatch
       /\ LET m == [seq |-> ws[w].next,
                    rec |-> [epoch |-> ws[w].epoch, batch |-> B, fence |-> FALSE, torn |-> FALSE]]
          IN /\ inflight' = inflight \cup {m}
             /\ ws' = [ws EXCEPT ![w].mode = "wait", ![w].out = m]
       /\ used' = used \cup B
  /\ UNCHANGED <<segs, mans, acked, corrupt>>

\* the store executes a request (create-only)
Land(m) ==
  /\ m \in inflight
  /\ segs' = IF Present(m.seq) THEN segs ELSE [segs EXCEPT ![m.seq] = m.rec]
  /\ inflight' = inflight \ {m}
  /\ UNCHANGED <<mans, ws, acked, used, corrupt>>

\* the request is lost
Drop(m) ==
  /\ m \in inflight
  /\ inflight' = inflight \ {m}
  /\ UNCHANGED <<segs, mans, ws, acked, used, corrupt>>

\* power loss while a crashed writer's PUT is being written (local FS only)
LandTorn(m) ==
  /\ m \in inflight
  /\ \A w \in Writers : ws[w].mode = "wait" => ws[w].out # m
  /\ ~Present(m.seq)
  /\ \E keep \in SUBSET m.rec.batch :
       segs' = [segs EXCEPT ![m.seq] = [m.rec EXCEPT !.batch = keep, !.torn = TRUE]]
  /\ inflight' = inflight \ {m}
  /\ UNCHANGED <<mans, ws, acked, used, corrupt>>

\* putExactlyOnce: response arrives / read-back after a 412 or timeout
Reply(w) ==
  /\ ws[w].mode = "wait"
  /\ ws[w].out \notin inflight
  /\ LET m == ws[w].out IN
     IF segs[m.seq] = m.rec
     THEN /\ acked' = acked \cup m.rec.batch            \* durable: ack
          /\ ws' = [ws EXCEPT ![w].mode = "open", ![w].next = m.seq + 1]
          /\ inflight' = inflight
     ELSE IF Present(m.seq)
          THEN /\ ws' = [ws EXCEPT ![w].mode = "dead"]   \* ErrFenced
               /\ UNCHANGED <<acked, inflight>>
          ELSE /\ inflight' = inflight \cup {m}          \* lost: retry same bytes
               /\ UNCHANGED <<ws, acked>>
  /\ UNCHANGED <<segs, mans, used, corrupt>>

\* retries exhausted while the PUT may still be in flight: ErrUnknownOutcome
GiveUp(w) ==
  /\ ws[w].mode = "wait"
  /\ ws' = [ws EXCEPT ![w].mode = "dead"]
  /\ UNCHANGED <<segs, mans, inflight, acked, used, corrupt>>

Crash(w) ==
  /\ ws[w].mode # "down"
  /\ ws' = [ws EXCEPT ![w] = Idle]
  /\ UNCHANGED <<segs, mans, inflight, acked, used, corrupt>>

Next ==
  \/ \E w \in Writers : ReadManifest(w) \/ PutManifest(w) \/ Scan(w) \/ Fence(w)
                        \/ Send(w) \/ Reply(w) \/ GiveUp(w) \/ Crash(w)
  \/ \E m \in inflight : Land(m) \/ Drop(m) \/ LandTorn(m)

Spec == Init /\ [][Next]_vars

\* ------------------------------ properties ------------------------------
\* every acknowledged transaction is in the recovered (dense) log
Durability == acked \subseteq Committed
\* the log has no holes: seq s exists only if 1..s-1 exist
NoHoles == \A s \in Seqs : Present(s) => \A t \in 1..(s-1) : Present(t)
\* writer epochs never go backwards along the log: a fenced (older) writer
\* can never append after a newer writer's fence  => no split brain
EpochMonotone == \A s, t \in Seqs : (s < t /\ Present(s) /\ Present(t)) => segs[s].epoch <= segs[t].epoch
\* a transaction is applied at most once
NoDoubleApply == \A s, t \in Seqs : (s # t /\ Present(s) /\ Present(t)) => segs[s].batch \cap segs[t].batch = {}
\* acknowledged data is never in a torn segment, and torn data only at the tail
NoCorruption == ~corrupt
TornOnlyUnacked == \A s \in Seqs : (Present(s) /\ segs[s].torn) => segs[s].batch \cap acked = {}
\* at most one writer can hold the newest epoch; older ones cannot get acks
SingleLeader == \A s \in Seqs : (Present(s) /\ ~segs[s].fence /\ segs[s].batch \cap acked # {})
                  => \A t \in (s+1)..MaxSeq : Present(t) => segs[t].epoch >= segs[s].epoch
\* manifest epochs strictly increase with ids
ManifestMonotone == \A i \in 1..Len(mans) : i > 1 => mans[i] > mans[i-1]

\* temporal: acked is never retracted, segments are immutable once written
AckedStable == [][acked \subseteq acked']_vars
Immutable == [][\A s \in Seqs : Present(s) => (segs'[s] = segs[s] \/ (segs[s].torn /\ segs'[s] = [segs[s] EXCEPT !.torn = FALSE]))]_vars
=============================================================================
