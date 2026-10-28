---- MODULE FreshANN ----
\* Persisted HNSW graphs (docs/design/10-hnsw-persist.md). A graph is a set
\* of (id, version) nodes; a snapshot is a saved graph. Restore keeps a
\* node only if it matches the current vector (hash check), then inserts
\* the rest. A zombie (fenced by a takeover) may still save on Close: it
\* checks the fence before and after its put and deletes its snapshot if
\* fenced. Shred deletes all snapshots and stops saves until a rebuild.
EXTENDS Naturals, FiniteSets
CONSTANTS Ids, MaxVer, VALIDATE, RECHECK

VARIABLES val, live, shredded, g, up, tainted, snaps, zpc, zg, fenced
vars == <<val, live, shredded, g, up, tainted, snaps, zpc, zg, fenced>>

Cur == {<<i, val[i]>> : i \in {j \in Ids : live[j]}}
Snap(vs, z) == [vecs |-> vs, zombie |-> z]

Init == /\ val = [i \in Ids |-> 0] /\ live = [i \in Ids |-> FALSE]
        /\ shredded = {} /\ g = {} /\ up = TRUE /\ tainted = FALSE
        /\ snaps = {} /\ zpc = "none" /\ zg = {} /\ fenced = TRUE

\* the owner may act only once it has fenced out the zombie
Owner == up /\ fenced

Put(i) == /\ Owner /\ val[i] < MaxVer
          /\ val' = [val EXCEPT ![i] = @ + 1] /\ live' = [live EXCEPT ![i] = TRUE]
          /\ g' = {n \in g : n[1] # i} \cup {<<i, val[i] + 1>>}
          /\ UNCHANGED <<shredded, up, tainted, snaps, zpc, zg, fenced>>
Del(i) == /\ Owner /\ live[i]
          /\ live' = [live EXCEPT ![i] = FALSE] /\ g' = {n \in g : n[1] # i}
          /\ UNCHANGED <<val, shredded, up, tainted, snaps, zpc, zg, fenced>>
Shred(i) == /\ Owner /\ live[i]
            /\ shredded' = shredded \cup {<<i, val[i]>>}
            /\ live' = [live EXCEPT ![i] = FALSE] /\ g' = {n \in g : n[1] # i}
            /\ snaps' = {} /\ tainted' = TRUE
            /\ UNCHANGED <<val, up, zpc, zg, fenced>>
Save == /\ Owner /\ ~tainted /\ snaps' = snaps \cup {Snap(g, FALSE)}
        /\ UNCHANGED <<val, live, shredded, g, up, tainted, zpc, zg, fenced>>
Crash == /\ up /\ up' = FALSE /\ g' = {}
         /\ UNCHANGED <<val, live, shredded, tainted, snaps, zpc, zg, fenced>>
Restore(s) == LET kept == {n \in s : ~VALIDATE \/ n \in Cur}
              IN kept \cup {c \in Cur : \A n \in kept : n[1] # c[1]}
Open == /\ ~up /\ up' = TRUE /\ tainted' = FALSE
        /\ \E s \in {x.vecs : x \in snaps} \cup {{}} : g' = Restore(s)
        /\ UNCHANGED <<val, live, shredded, snaps, zpc, zg, fenced>>
\* a second opener takes over: the old one becomes a zombie holding g
Takeover == /\ up /\ fenced /\ zpc = "none"
            /\ zpc' = "check1" /\ zg' = g /\ fenced' = FALSE
            /\ UNCHANGED <<val, live, shredded, g, up, tainted, snaps>>
Fence == /\ ~fenced /\ fenced' = TRUE
         /\ UNCHANGED <<val, live, shredded, g, up, tainted, snaps, zpc, zg>>
ZCheck1 == /\ zpc = "check1" /\ zpc' = IF fenced THEN "done" ELSE "put"
           /\ UNCHANGED <<val, live, shredded, g, up, tainted, snaps, zg, fenced>>
ZPut == /\ zpc = "put" /\ snaps' = snaps \cup {Snap(zg, TRUE)}
        /\ zpc' = IF RECHECK THEN "check2" ELSE "done"
        /\ UNCHANGED <<val, live, shredded, g, up, tainted, zg, fenced>>
ZCheck2 == /\ zpc = "check2" /\ zpc' = "done"
           /\ snaps' = IF fenced THEN snaps \ {Snap(zg, TRUE)} ELSE snaps
           /\ UNCHANGED <<val, live, shredded, g, up, tainted, zg, fenced>>

Next == \/ \E i \in Ids : Put(i) \/ Del(i) \/ Shred(i)
        \/ Save \/ Crash \/ Open \/ Takeover \/ Fence
        \/ ZCheck1 \/ ZPut \/ ZCheck2

\* the in-memory graph covers exactly the current vectors
GraphCurrent == up => g = Cur
\* no saved graph holds a shredded vector, except a zombie's that is
\* about to be deleted by its own fence re-check
ShredClean == \A s \in snaps : s.vecs \cap shredded # {} => (s.zombie /\ zpc = "check2")

Spec == Init /\ [][Next]_vars
====
