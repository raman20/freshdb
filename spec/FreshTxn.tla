---------------------------- MODULE FreshTxn ----------------------------
(***************************************************************************)
(* Serializable OCC transactions across lenses (writer.go conflict()),    *)
(* the "consume a message + write KV + ack cursor in one txn" pattern.    *)
(*                                                                         *)
(* A txn reads at snapshot LSN `snap`, records its read set, and at commit *)
(* the single writer aborts it if any key in its read set was written at   *)
(* an LSN > snap (blind writes never conflict). A committed txn is ONE log *)
(* entry, so all its lens effects (topic cursor, KV) are atomic; a crash   *)
(* loses a suffix of entries, never part of one.                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Clients, NMsgs, MaxLog

Keys == {"cursor", "x"}
\* an entry: the keys it read (and the values it saw), the keys it wrote
VARIABLES log, cs, durable

vars == <<log, cs, durable>>

\* fold of a log prefix: the value of the cursor and processed counts
Cursor(l) == LET RECURSIVE C(_) C(s) == IF s = <<>> THEN 0
                                ELSE IF Head(s).kind = "consume" THEN C(Tail(s)) + 1 ELSE C(Tail(s))
             IN C(l)
Prefix(n) == SubSeq(log, 1, n)
Writes(e) == IF e.kind = "consume" THEN {"cursor", "x"} ELSE {"x"}
LastWrite(k, n) == \* does any entry after n write k?
  \E i \in (n+1)..Len(log) : k \in Writes(log[i])

Init ==
  /\ log = <<>>
  /\ cs = [c \in Clients |-> [st |-> "idle", snap |-> 0, saw |-> 0, kind |-> "none"]]
  /\ durable = 0

\* consumer: read cursor at snapshot, process message `saw`, write done/<saw>, ack
BeginConsume(c) ==
  /\ cs[c].st = "idle"
  /\ Cursor(log) < NMsgs
  /\ cs' = [cs EXCEPT ![c] = [st |-> "active", snap |-> Len(log), saw |-> Cursor(log), kind |-> "consume"]]
  /\ UNCHANGED <<log, durable>>

\* blind writer: writes x without reading
BeginBlind(c) ==
  /\ cs[c].st = "idle"
  /\ \A d \in Clients : cs[d].kind # "blind" \/ cs[d].st = "idle"   \* at most one blind writer at a time (bounds the state space)
  /\ cs' = [cs EXCEPT ![c] = [st |-> "active", snap |-> Len(log), saw |-> 0, kind |-> "blind"]]
  /\ UNCHANGED <<log, durable>>

Commit(c) ==
  /\ cs[c].st = "active"
  /\ Len(log) < MaxLog
  /\ LET t == cs[c]
         reads  == IF t.kind = "consume" THEN {"cursor"} ELSE {}
     IN IF \E k \in reads : LastWrite(k, t.snap)
        THEN /\ cs' = [cs EXCEPT ![c].st = "idle"]           \* ErrConflict
             /\ log' = log
        ELSE /\ log' = Append(log, [kind |-> t.kind, msg |-> t.saw])
             /\ cs' = [cs EXCEPT ![c].st = "idle"]
  /\ UNCHANGED durable

Abort(c) ==
  /\ cs[c].st = "active"
  /\ cs' = [cs EXCEPT ![c].st = "idle"]
  /\ UNCHANGED <<log, durable>>

\* group commit makes a prefix durable; a crash loses the non-durable suffix
Sync == /\ durable < Len(log) /\ durable' = Len(log) /\ UNCHANGED <<log, cs>>
Crash ==
  /\ log' = SubSeq(log, 1, durable)
  /\ cs' = [c \in Clients |-> [st |-> "idle", snap |-> 0, saw |-> 0, kind |-> "none"]]
  /\ UNCHANGED durable

Next == \/ \E c \in Clients : BeginConsume(c) \/ BeginBlind(c) \/ Commit(c) \/ Abort(c)
        \/ Sync \/ Crash

Spec == Init /\ [][Next]_vars

\* ---- properties ----
Consumes == { i \in 1..Len(log) : log[i].kind = "consume" }
\* exactly-once: every message is processed at most once ...
ExactlyOnce == \A i, j \in Consumes : i # j => log[i].msg # log[j].msg
\* ... and in order with no gaps: processed set == {0..cursor-1}
AtomicCursor == { log[i].msg : i \in Consumes } = 0..(Cursor(log) - 1)
\* serializability: every committed txn's reads equal the state produced by
\* the serial order (the log) just before it
Serializable == \A i \in Consumes : log[i].msg = Cursor(Prefix(i - 1))
DurablePrefix == durable <= Len(log)
=============================================================================
