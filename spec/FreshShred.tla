---------------------------- MODULE FreshShred ----------------------------
(***************************************************************************)
(* Crypto-shredding + hash-chained audit log (keyring.go, shred.go).      *)
(*                                                                         *)
(* Values of subject s are stored as ciphertext under DEK(s). Shred(s):    *)
(*   1. create-only marker keys/shredded/s   (the commit point)            *)
(*   2. delete keys/dek/s                     (may crash before this)      *)
(*   3. append an OpShred audit entry; certificate = chain head at it      *)
(* A DEK create PUT may still be IN FLIGHT and land after step 2 (found by *)
(* the deterministic simulator); open/Verify sweep such late DEKs.        *)
(* Reads go through the engine: readable iff no marker AND DEK present.    *)
(* The chain: chain[i] = H(chain[i-1], digest(ciphertext_i)); shredding    *)
(* never rewrites the log, so the chain stays verifiable.                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Subjects, MaxLog

VARIABLES log, chain, dek, marker, inflight, revealed, certs, phase
vars == <<log, chain, dek, marker, inflight, revealed, certs, phase>>

H(prev, ct) == <<prev, ct>>          \* abstract collision-free hash
Genesis == "genesis"
ChainOf(l) == LET RECURSIVE C(_) C(n) == IF n = 0 THEN Genesis ELSE H(C(n-1), l[n].ct)
              IN [i \in 1..Len(l) |-> C(i)]

Init ==
  /\ log = <<>> /\ chain = <<>>
  /\ dek = [s \in Subjects |-> FALSE]
  /\ marker = [s \in Subjects |-> FALSE]
  /\ inflight = {}
  /\ revealed = {}
  /\ certs = {}
  /\ phase = [s \in Subjects |-> "none"]

AppendEntry(e) ==
  /\ log' = Append(log, e)
  /\ chain' = Append(chain, H(IF Len(chain) = 0 THEN Genesis ELSE chain[Len(chain)], e.ct))

\* writer needs a DEK: sends a create-only PUT (may land late)
RequestDEK(s) ==
  /\ ~marker[s] /\ ~dek[s] /\ s \notin inflight
  /\ inflight' = inflight \cup {s}
  /\ UNCHANGED <<log, chain, dek, marker, revealed, certs, phase>>

LandDEK(s) ==
  /\ s \in inflight
  /\ dek' = [dek EXCEPT ![s] = TRUE]
  /\ inflight' = inflight \ {s}
  /\ UNCHANGED <<log, chain, marker, revealed, certs, phase>>

Write(s) ==
  /\ Len(log) < MaxLog
  /\ ~marker[s] /\ dek[s]                   \* tx.write/seal refuse shredded subjects
  /\ AppendEntry([subj |-> s, ct |-> <<s, Len(log) + 1>>, audit |-> FALSE])
  /\ UNCHANGED <<dek, marker, inflight, revealed, certs, phase>>

ShredMark(s) ==
  /\ phase[s] = "none"
  /\ marker' = [marker EXCEPT ![s] = TRUE]
  /\ phase' = [phase EXCEPT ![s] = "marked"]
  /\ UNCHANGED <<log, chain, dek, inflight, revealed, certs>>

ShredDelete(s) ==
  /\ phase[s] = "marked"
  /\ dek' = [dek EXCEPT ![s] = FALSE]
  /\ phase' = [phase EXCEPT ![s] = "deleted"]
  /\ UNCHANGED <<log, chain, marker, inflight, revealed, certs>>

ShredAudit(s) ==
  /\ phase[s] = "deleted" /\ Len(log) < MaxLog
  /\ AppendEntry([subj |-> s, ct |-> <<"audit", s>>, audit |-> TRUE])
  /\ certs' = certs \cup {[subj |-> s, lsn |-> Len(log) + 1,
                           head |-> H(IF Len(chain) = 0 THEN Genesis ELSE chain[Len(chain)], <<"audit", s>>)]}
  /\ phase' = [phase EXCEPT ![s] = "done"]
  /\ UNCHANGED <<dek, marker, inflight, revealed>>

\* crash between marker and delete: the next open finishes the shred (keyring.go)
CrashAndOpen ==
  /\ phase' = [s \in Subjects |-> IF phase[s] = "marked" THEN "deleted" ELSE phase[s]]
  /\ dek' = [s \in Subjects |-> dek[s] /\ ~marker[s]]
  /\ UNCHANGED <<log, chain, marker, inflight, revealed, certs>>

\* open/Verify sweep: delete DEKs of shredded subjects (late landings)
Sweep(s) ==
  /\ marker[s] /\ dek[s]
  /\ dek' = [dek EXCEPT ![s] = FALSE]
  /\ UNCHANGED <<log, chain, marker, inflight, revealed, certs, phase>>

\* engine read of entry i (any branch / any time-travel position: same log)
Read(i) ==
  /\ i \in 1..Len(log) /\ ~log[i].audit
  /\ LET s == log[i].subj IN
     /\ ~marker[s] /\ dek[s]
     /\ revealed' = revealed \cup {[subj |-> s, afterShred |-> marker[s]]}
  /\ UNCHANGED <<log, chain, dek, marker, inflight, certs, phase>>

Next ==
  \/ \E s \in Subjects : RequestDEK(s) \/ LandDEK(s) \/ Write(s) \/ ShredMark(s)
                         \/ ShredDelete(s) \/ ShredAudit(s) \/ Sweep(s)
  \/ CrashAndOpen
  \/ \E i \in 1..MaxLog : Read(i)

Spec == Init /\ [][Next]_vars /\ \A s \in Subjects : WF_vars(Sweep(s)) /\ WF_vars(LandDEK(s))
                              /\ WF_vars(ShredDelete(s))

\* ---- properties ----
ShreddedNeverRevealed == \A r \in revealed : ~r.afterShred
NoWritesAfterShred == \A i \in 1..Len(log) : (~log[i].audit /\ marker[log[i].subj]) =>
                        \A j \in 1..Len(log) : (log[j].audit /\ log[j].subj = log[i].subj) => i < j
ChainVerifiable == chain = ChainOf(log)
CertsVerify == \A c \in certs : c.lsn <= Len(chain) /\ chain[c.lsn] = c.head
\* log is append-only: shredding never rewrites history (chain stays valid)
AppendOnly == [][SubSeq(log', 1, Len(log)) = log]_vars
\* liveness: once shredded, the key material is eventually gone for good,
\* even if a DEK create was in flight when the shred happened
KeyEventuallyGone == \A s \in Subjects : <>[](marker[s] => ~dek[s])
=============================================================================
