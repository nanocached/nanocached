---------------------------- MODULE JoinHandoff ----------------------------
(***************************************************************************)
(* One key during a staged join: the old owners of the key (Sources), the *)
(* joiner J, one client issuing writes one at a time, the join's transfer *)
(* and each source's forward queue to the joiner.                         *)
(*                                                                         *)
(* What each action stands for in the code (src/server.rs, src/cache.rs): *)
(*                                                                         *)
(*   Peek / Send   run_migration: peek_entry_for_handoff reads the live    *)
(*                 value, transfer_with_retries sends it as a              *)
(*                 put-if-absent `U ... A` on the transfer's own           *)
(*                 connection; a displaced source marks its copy after     *)
(*                 the send is acknowledged (mark_migrated).               *)
(*   ApplyLeg      a client write leg; on a source, forward_result pushes  *)
(*                 the result for the joiner (migration_targets_for,       *)
(*                 as_handoff = true, so `U` / `u` frames).                *)
(*   push          TargetQueue::push coalesces per key: with one key, the  *)
(*                 queue holds at most one waiting write, the newest.      *)
(*   Deliver       drain_forward_queue: one write at a time, in order.     *)
(*   at J          Cache::handoff_set (unconditional), handoff_delete      *)
(*                 (removes and tombstones), handoff_set_if_absent         *)
(*                 (refused when present or tombstoned).                   *)
(*   Sweep         run_sweep removing marked dead copies.                  *)
(*   Abandon       the abandon path: marks restored, forwards revoked.     *)
(*                                                                         *)
(* Left out: TTLs other than an expiring touch, tombstone expiry, the      *)
(* ownership reclaim pass, re-replication, eviction, decommission,         *)
(* lingering joiners, clears, multi-key batches, connection failures,     *)
(* concurrent clients, a forward in flight while the next one is queued   *)
(* (a pop and its delivery are one step here), sources learning of the    *)
(* confirmation at different times, the forwarding window closing.        *)
(* Ownership is fixed by the constants: before the join the owners are    *)
(* Sources, after it (Sources \ Displaced) \cup {J}.                       *)
(*                                                                         *)
(* The flags switch individual fixes off to check that the model finds    *)
(* the bug each one fixed:                                                 *)
(*   TOMB_DELETE   a forwarded delete tombstones the key on the joiner.   *)
(*   TOMB_EXPIRY   a forwarded expiry (touch with TTL 0) does the same.   *)
(*   HOLD_MARKS    dead copies are swept only once the join is confirmed. *)
(* POST_CONFIRM_WRITES lets the client write after the promotion, by the  *)
(* old list (stale) or the new one (fresh).                                *)
(* W_RETRY: a leg a displaced source answers W after the confirmation is  *)
(* retried on J (the SDK refreshes and retries). FALSE drops the leg, as  *)
(* the SDKs do for a refused replica leg (accepted behaviour).            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Sources, Displaced, J, MaxOps,
          TOMB_DELETE, TOMB_EXPIRY, HOLD_MARKS, POST_CONFIRM_WRITES,
          W_RETRY

ASSUME Displaced \subseteq Sources /\ J \notin Sources /\ MaxOps \in Nat

Nodes   == Sources \cup {J}
Absent  == 0          \* no live entry (deleted, expired, never written)
Initial == 1          \* the value the key held before the join
\* Op n writes the value n + 1, so every written value is distinct.
Vals    == 1..(MaxOps + 1)
Kinds   == {"set", "del", "expire"}

OldOwners == Sources
NewOwners == (Sources \ Displaced) \cup {J}

VARIABLES
    store,    \* [Nodes -> Vals \cup {Absent}]
    jsrc,     \* which path wrote store[J]: "none", "xfer", "fwd", "client"
    tomb,     \* the joiner holds a handoff tombstone for the key
    xst,      \* [Sources -> {"idle", "peeked", "done"}]: the transfer
    xval,     \* [Sources -> Vals \cup {Absent}]: the value it peeked
    marked,   \* [Sources -> BOOLEAN]: dead-copy mark
    fwd,      \* [Sources -> Seq(msg)]: the forward queue to J
    phase,    \* "transfer", "completed", "confirmed", "abandoned"
    ops,      \* client ops started so far
    legs,     \* nodes the current op has yet to reach
    cur,      \* the current op: [kind |-> ..., val |-> ...]
    final,    \* what the last op left: its value, or Absent
    raced     \* history: J took a direct client write while an older
              \* forward was still queued for it

vars == <<store, jsrc, tomb, xst, xval, marked, fwd, phase, ops, legs, cur, final, raced>>

Msg(kind, val) == [kind |-> kind, val |-> val]

TypeOK ==
    /\ store \in [Nodes -> Vals \cup {Absent}]
    /\ jsrc \in {"none", "xfer", "fwd", "client"}
    /\ tomb \in BOOLEAN
    /\ xst \in [Sources -> {"idle", "peeked", "done"}]
    /\ xval \in [Sources -> Vals \cup {Absent}]
    /\ marked \in [Sources -> BOOLEAN]
    /\ \A s \in Sources : Len(fwd[s]) <= 1
    /\ phase \in {"transfer", "completed", "confirmed", "abandoned"}
    /\ legs \subseteq Nodes

Init ==
    /\ store = [n \in Nodes |-> IF n = J THEN Absent ELSE Initial]
    /\ jsrc = "none"
    /\ tomb = FALSE
    /\ xst = [s \in Sources |-> "idle"]
    /\ xval = [s \in Sources |-> Absent]
    /\ marked = [s \in Sources |-> FALSE]
    /\ fwd = [s \in Sources |-> <<>>]
    /\ phase = "transfer"
    /\ ops = 0
    /\ legs = {}
    /\ cur = Msg("set", Initial)
    /\ final = Initial
    /\ raced = FALSE

Result(op) == IF op.kind = "set" THEN op.val ELSE Absent

(* The client starts its next op once the previous one reached every    *)
(* owner it was sent to. Until the promotion every client routes by the  *)
(* old list; after it, by either list.                                   *)
StartOp(kind, route) ==
    /\ legs = {}
    /\ ops < MaxOps
    /\ \/ phase \in {"transfer", "completed"} /\ route = "old"
       \/ phase = "confirmed" /\ POST_CONFIRM_WRITES
    /\ LET op == Msg(kind, ops + 2) IN
         /\ cur' = op
         /\ final' = Result(op)
    /\ legs' = IF route = "old" THEN OldOwners ELSE NewOwners
    /\ ops' = ops + 1
    /\ UNCHANGED <<store, jsrc, tomb, xst, xval, marked, fwd, phase, raced>>

ForwardOpen == phase \in {"transfer", "completed", "confirmed"}

(* A leg reaching a source: applied locally, and pushed for the joiner   *)
(* while the forwarding window is open. A displaced source that has seen *)
(* the confirmation answers W and changes nothing; the leg then goes to  *)
(* J (W_RETRY) or is dropped. A leg reaching J directly is a plain       *)
(* client write: no tombstone either way.                                *)
ApplyLeg(n) ==
    /\ n \in legs
    /\ legs' = IF n /= J /\ phase = "confirmed" /\ n \in Displaced /\ W_RETRY
                 THEN (legs \ {n}) \cup {J}
                 ELSE legs \ {n}
    /\ IF n = J
         THEN /\ store' = [store EXCEPT ![J] = Result(cur)]
              /\ jsrc' = "client"
              /\ raced' = (raced \/ \E s \in Sources : fwd[s] /= <<>>)
              /\ UNCHANGED <<fwd, marked>>
         ELSE /\ UNCHANGED raced
              /\ IF phase = "confirmed" /\ n \in Displaced
                   THEN UNCHANGED <<store, jsrc, fwd, marked>>
                   ELSE /\ store' = [store EXCEPT ![n] = Result(cur)]
                        /\ marked' = [marked EXCEPT ![n] = FALSE]
                        /\ fwd' = IF ForwardOpen
                                    THEN [fwd EXCEPT ![n] = <<cur>>]
                                    ELSE fwd
                        /\ UNCHANGED jsrc
    /\ UNCHANGED <<tomb, xst, xval, phase, ops, cur, final>>

(* The head of a source's forward queue lands on the joiner.             *)
Deliver(s) ==
    /\ fwd[s] /= <<>>
    /\ phase /= "abandoned"
    /\ LET m == Head(fwd[s]) IN
         CASE m.kind = "set" ->
                /\ store' = [store EXCEPT ![J] = m.val]
                /\ jsrc' = "fwd"
                /\ UNCHANGED tomb
           [] m.kind = "del" ->
                /\ store' = [store EXCEPT ![J] = Absent]
                /\ jsrc' = "fwd"
                /\ tomb' = (tomb \/ TOMB_DELETE)
           [] m.kind = "expire" ->
                /\ store' = [store EXCEPT ![J] = Absent]
                /\ jsrc' = "fwd"
                /\ tomb' = (tomb \/ TOMB_EXPIRY)
    /\ fwd' = [fwd EXCEPT ![s] = Tail(fwd[s])]
    /\ UNCHANGED <<xst, xval, marked, phase, ops, legs, cur, final, raced>>

(* The transfer reads the key; a source that holds nothing sends nothing *)
(* and marks nothing.                                                     *)
Peek(s) ==
    /\ phase = "transfer"
    /\ xst[s] = "idle"
    /\ IF store[s] = Absent
         THEN /\ xst' = [xst EXCEPT ![s] = "done"]
              /\ UNCHANGED xval
         ELSE /\ xst' = [xst EXCEPT ![s] = "peeked"]
              /\ xval' = [xval EXCEPT ![s] = store[s]]
    /\ UNCHANGED <<store, jsrc, tomb, marked, fwd, phase, ops, legs, cur, final, raced>>

(* The peeked copy lands put-if-absent; the sender marks its copy dead   *)
(* if it is displaced, refused or not.                                    *)
Send(s) ==
    /\ phase = "transfer"
    /\ xst[s] = "peeked"
    /\ IF tomb \/ store[J] /= Absent
         THEN UNCHANGED <<store, jsrc>>
         ELSE /\ store' = [store EXCEPT ![J] = xval[s]]
              /\ jsrc' = "xfer"
    /\ xst' = [xst EXCEPT ![s] = "done"]
    /\ marked' = [marked EXCEPT ![s] = (s \in Displaced)]
    /\ UNCHANGED <<tomb, xval, fwd, phase, ops, legs, cur, final, raced>>

Complete ==
    /\ phase = "transfer"
    /\ \A s \in Sources : xst[s] = "done"
    /\ phase' = "completed"
    /\ UNCHANGED <<store, jsrc, tomb, xst, xval, marked, fwd, ops, legs, cur, final, raced>>

Confirm ==
    /\ phase = "completed"
    /\ phase' = "confirmed"
    /\ UNCHANGED <<store, jsrc, tomb, xst, xval, marked, fwd, ops, legs, cur, final, raced>>

(* Discovery gives the join up, mid-transfer or after completion: marks  *)
(* are restored, queued forwards revoked, the transfer stops.             *)
Abandon ==
    /\ phase \in {"transfer", "completed"}
    /\ phase' = "abandoned"
    /\ marked' = [s \in Sources |-> FALSE]
    /\ fwd' = [s \in Sources |-> <<>>]
    /\ xst' = [s \in Sources |-> "done"]
    /\ UNCHANGED <<store, jsrc, tomb, xval, ops, legs, cur, final, raced>>

SweepAllowed == IF HOLD_MARKS THEN phase = "confirmed"
                              ELSE phase \in {"completed", "confirmed"}

Sweep(s) ==
    /\ marked[s]
    /\ SweepAllowed
    /\ store' = [store EXCEPT ![s] = Absent]
    /\ marked' = [marked EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<jsrc, tomb, xst, xval, fwd, phase, ops, legs, cur, final, raced>>

Next ==
    \/ \E k \in Kinds, r \in {"old", "new"} : StartOp(k, r)
    \/ \E n \in Nodes : ApplyLeg(n)
    \/ \E s \in Sources : Deliver(s) \/ Peek(s) \/ Send(s) \/ Sweep(s)
    \/ Complete \/ Confirm \/ Abandon

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Nothing is in flight: the op reached every leg, the transfer and the  *)
(* queues are empty, the join is decided and its sweep has run.          *)
Quiescent ==
    /\ legs = {}
    /\ \A s \in Sources : xst[s] = "done" /\ fwd[s] = <<>>
    /\ phase \in {"confirmed", "abandoned"}
    /\ ~(phase = "confirmed" /\ \E s \in Sources : marked[s])

(* After a confirmed join the joiner serves what the last write left,    *)
(* unless a direct write raced an older queued forward: the forward is   *)
(* unconditional and overwrites it (accepted behaviour, see              *)
(* JoinerHoldsFinalStrict).                                               *)
JoinerHoldsFinal ==
    Quiescent /\ phase = "confirmed" /\ ~raced => store[J] = final

(* The same without the exception; expected to be violated.               *)
JoinerHoldsFinalStrict ==
    Quiescent /\ phase = "confirmed" => store[J] = final

(* Narrower: whatever the joiner kept from the transfer's put-if-absent  *)
(* copy is not older than the last write. A lagging forward overwriting  *)
(* a direct write (accepted behaviour) does not violate this one.        *)
TransferCopyNotStale ==
    Quiescent /\ phase = "confirmed" /\ jsrc = "xfer" => store[J] = final

(* The new owners other than J keep what the last write left.            *)
SurvivorsHoldFinal ==
    Quiescent /\ phase = "confirmed" =>
        \A s \in Sources \ Displaced : store[s] = final

(* An abandoned join leaves every old owner with what the last write     *)
(* left: no copy is lost to the half-finished handoff.                   *)
AbandonKeepsCopies ==
    Quiescent /\ phase = "abandoned" => \A s \in Sources : store[s] = final

=============================================================================
