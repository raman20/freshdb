---------------------------- MODULE FreshRetain ----------------------------
(* Topic retention vs consumer groups (compact.go topicCuts, tx.go Group).
   One topic; each group's workers consume concurrently in transactions: Fetch reads the group's
   cursor (into the read set) and the message there (Read clamps an offset
   below the retained start to the start), Commit processes it and writes
   cursor = offset+1, aborting if the cursor changed since the read (OCC,
   as in FreshTxn). Compaction may move the retained start to any offset
   up to the end, lowered to the smallest committed cursor when Clamp.
   Writers are paused during compaction, so it is one atomic step that
   can fall between a Fetch and its Commit.

   ExactlyOnce: every group has processed each offset below its cursor
   exactly once and none at or above it. With Clamp = FALSE (the
   IgnoreGroups option) a lagging group skips messages: violated. *)
EXTENDS Naturals, FiniteSets

CONSTANTS Groups, Workers, MaxMsgs, Clamp

VARIABLES len, start, cursor, pending, done
vars == <<len, start, cursor, pending, done>>

None == MaxMsgs + 1
Offs == 0..MaxMsgs

Init ==
  /\ len = 0 /\ start = 0
  /\ cursor = [g \in Groups |-> 0]
  /\ pending = [p \in Groups \X Workers |-> [read |-> None, off |-> None]]
  /\ done = [g \in Groups |-> [o \in Offs |-> 0]]

Publish == len < MaxMsgs /\ len' = len + 1 /\ UNCHANGED <<start, cursor, pending, done>>

\* Fetch(1): the message at the cursor, or at the start if retention
\* removed it.
Fetch(g, w) ==
  /\ pending[<<g, w>>].read = None
  /\ LET o == IF cursor[g] < start THEN start ELSE cursor[g] IN
       /\ o < len
       /\ pending' = [pending EXCEPT ![<<g, w>>] = [read |-> cursor[g], off |-> o]]
  /\ UNCHANGED <<len, start, cursor, done>>

Commit(g, w) ==
  LET p == pending[<<g, w>>] IN
  /\ p.read # None
  /\ IF cursor[g] = p.read
       THEN /\ done' = [done EXCEPT ![g][p.off] = @ + 1]
            /\ cursor' = [cursor EXCEPT ![g] = p.off + 1]
       ELSE UNCHANGED <<done, cursor>>
  /\ pending' = [pending EXCEPT ![<<g, w>>] = [read |-> None, off |-> None]]
  /\ UNCHANGED <<len, start>>

\* Abort: the transaction gives up (or the process crashes) after Fetch.
Abort(g, w) ==
  /\ pending[<<g, w>>].read # None
  /\ pending' = [pending EXCEPT ![<<g, w>>] = [read |-> None, off |-> None]]
  /\ UNCHANGED <<len, start, cursor, done>>

MinCursor == CHOOSE c \in {cursor[g] : g \in Groups} : \A g \in Groups : c <= cursor[g]

Compact ==
  \E cut \in start..len :
    /\ (Clamp => cut <= MinCursor)
    /\ start' = cut
    /\ UNCHANGED <<len, cursor, pending, done>>

Next == Publish \/ Compact \/ \E g \in Groups, w \in Workers : Fetch(g, w) \/ Commit(g, w) \/ Abort(g, w)
Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ len \in 0..MaxMsgs /\ start \in 0..len
  /\ cursor \in [Groups -> 0..MaxMsgs]
  /\ done \in [Groups -> [Offs -> 0..2]]

ExactlyOnce == \A g \in Groups, o \in Offs :
  done[g][o] = IF o < cursor[g] THEN 1 ELSE 0

\* Retention never moves the start past an unacked message.
NoLoss == Clamp => \A g \in Groups : start <= cursor[g]
=============================================================================
