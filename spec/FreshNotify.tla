---------------------------- MODULE FreshNotify ----------------------------
(* Change notification for consumers (docs/design/06-push-consumers.md).
   Writer: the head changes (commit or rewind), then, as a separate step,
   the current channel is closed and replaced (gen + 1).
   Waiter (WaitHead): take the current channel, read the head; if it
   differs from what the waiter last saw, return it; otherwise block until
   the generation moves past the one it took, then loop.
   NoLostWakeup: a blocked waiter whose view is stale always has a wake-up
   coming. BUG = TRUE reads the head before taking the channel; TLC must
   report a violation for it (FreshNotifyBug.cfg). *)
EXTENDS Naturals

CONSTANTS Waiters, MaxHead, MaxSteps, BUG

VARIABLES head, gen, bump, steps, pc, took, rd, seen

vars == <<head, gen, bump, steps, pc, took, rd, seen>>

Init ==
  /\ head = 0 /\ gen = 0 /\ bump = FALSE /\ steps = 0
  /\ pc = [w \in Waiters |-> "s1"]
  /\ took = [w \in Waiters |-> 0]
  /\ rd = [w \in Waiters |-> 0]
  /\ seen = [w \in Waiters |-> 0]

Change(h) ==
  /\ steps < MaxSteps /\ ~bump /\ h /= head
  /\ head' = h /\ bump' = TRUE /\ steps' = steps + 1
  /\ UNCHANGED <<gen, pc, took, rd, seen>>

Bump ==
  /\ bump /\ gen' = gen + 1 /\ bump' = FALSE
  /\ UNCHANGED <<head, steps, pc, took, rd, seen>>

Decide(w, h) ==
  IF h /= seen[w]
    THEN seen' = [seen EXCEPT ![w] = h] /\ pc' = [pc EXCEPT ![w] = "s1"]
    ELSE pc' = [pc EXCEPT ![w] = "block"] /\ UNCHANGED seen

\* first step: take the channel (correct) or read the head (BUG)
S1(w) ==
  /\ pc[w] = "s1"
  /\ IF BUG THEN rd' = [rd EXCEPT ![w] = head] /\ UNCHANGED took
            ELSE took' = [took EXCEPT ![w] = gen] /\ UNCHANGED rd
  /\ pc' = [pc EXCEPT ![w] = "s2"]
  /\ UNCHANGED <<head, gen, bump, steps, seen>>

\* second step: the other half, then decide on the head read
S2(w) ==
  /\ pc[w] = "s2"
  /\ IF BUG THEN took' = [took EXCEPT ![w] = gen] /\ Decide(w, rd[w]) /\ UNCHANGED rd
            ELSE Decide(w, head) /\ UNCHANGED <<took, rd>>
  /\ UNCHANGED <<head, gen, bump, steps>>

Wake(w) ==
  /\ pc[w] = "block" /\ gen > took[w]
  /\ pc' = [pc EXCEPT ![w] = "s1"]
  /\ UNCHANGED <<head, gen, bump, steps, took, rd, seen>>

Next ==
  \/ \E h \in 0..MaxHead : Change(h)
  \/ Bump
  \/ \E w \in Waiters : S1(w) \/ S2(w) \/ Wake(w)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ head \in 0..MaxHead /\ gen \in 0..MaxSteps /\ bump \in BOOLEAN
  /\ pc \in [Waiters -> {"s1", "s2", "block"}]

NoLostWakeup ==
  \A w \in Waiters :
    (pc[w] = "block" /\ seen[w] /= head) => (gen > took[w] \/ bump)
=============================================================================
