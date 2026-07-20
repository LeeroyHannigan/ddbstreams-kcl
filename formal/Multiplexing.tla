---------------------------- MODULE Multiplexing ----------------------------
(***************************************************************************)
(* Formal model of `max_processing_concurrency` (multiplexing) in the      *)
(* amazon-dynamodb-streams-consumer worker, covering the full feature:      *)
(*                                                                          *)
(*   - a shared per-worker pool of `cap[w]` processing slots (the tokio     *)
(*     Semaphore in `process_shard`) bounds concurrent delivery;            *)
(*   - ONLINE RESIZE of that cap (`set_max_processing_concurrency`): grow   *)
(*     freely, shrink never below the in-flight count (shrink waits for a   *)
(*     slot to free), so the bound is never violated across a resize;       *)
(*   - MULTI-WORKER lease ownership: each shard is owned by <= 1 worker;    *)
(*     only the owner processes it, giving global mutual exclusion (no      *)
(*     split-brain) with a per-worker cap; leases are lost on crash or      *)
(*     handed off at a batch boundary (steal/expiry);                        *)
(*   - RESHARD: a child shard is gated on its parent completing             *)
(*     (parent-before-child), and is never split across slots;             *)
(*   - CHECKPOINT/at-least-once across crash + lease handoff.               *)
(*                                                                          *)
(* Properties (see Multiplexing.cfg):                                       *)
(*   PerWorkerBound  - each worker runs <= cap[w] shards at once, always,   *)
(*                     including during/after an online resize.             *)
(*   MutualExclusion - no shard is processed by two workers at once.        *)
(*   OwnedWhileProc  - a shard in flight on w is owned by w.                *)
(*   ParentBeforeChild - a child is only processed after its parent done.   *)
(*   CheckpointOK    - per-shard checkpoint stays in 0..MaxSeq, never skips. *)
(*   AtLeastOnce     - every checkpointed record was delivered >= once.     *)
(*   Termination     - every shard (incl. children) is eventually fully     *)
(*                     processed: no starvation, no permanent loss.         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    MaxSeq,      \* records per shard
    MaxCap,      \* max processing-concurrency cap a worker may be resized to
    MaxCrashes,  \* bound on crash actions (keeps the state space finite)
    MaxHandoffs  \* bound on voluntary lease handoffs (steal/expiry)

(* Small fixed topology for the check (two workers; two root shards; one    *)
(* child of r1, exercising reshard/parent-before-child). Encoded as strings *)
(* so the .cfg only carries numeric bounds.                                 *)
Workers  == {"w1", "w2"}
Roots    == {"r1", "r2"}
Leaves   == {"c1"}
ParentOf == { <<"c1", "r1">> }
NONE     == "NONE"

Shards == Roots \cup Leaves

ASSUME CapOK == MaxCap \in Nat /\ MaxCap >= 1
ASSUME SeqOK == MaxSeq \in Nat /\ MaxSeq >= 1

VARIABLES
    owner,       \* [Shards -> Workers \cup {NONE}] current lease holder
    inflight,    \* [Workers -> SUBSET Shards] shards each worker is processing
    cap,         \* [Workers -> 1..MaxCap] current per-worker concurrency cap
    checkpoint,  \* [Shards -> 0..MaxSeq] durable per-shard progress
    delivered,   \* [Shards -> Nat] total deliveries (>= checkpoint => dupes ok)
    crashes,     \* crash counter (bounded)
    handoffs     \* voluntary-handoff counter (bounded)

vars == <<owner, inflight, cap, checkpoint, delivered, crashes, handoffs>>

TypeOK ==
    /\ owner      \in [Shards -> Workers \cup {NONE}]
    /\ inflight   \in [Workers -> SUBSET Shards]
    /\ cap        \in [Workers -> 1..MaxCap]
    /\ checkpoint \in [Shards -> 0..MaxSeq]
    /\ delivered  \in [Shards -> 0..(MaxSeq + MaxCrashes)]
    /\ crashes    \in 0..MaxCrashes
    /\ handoffs   \in 0..MaxHandoffs

(* A child may be worked only once its parent is fully checkpointed. Roots  *)
(* (no matching pair) are always parent-complete.                           *)
ParentComplete(s) == \A pr \in ParentOf : (pr[1] = s) => checkpoint[pr[2]] = MaxSeq
Eligible(s) == checkpoint[s] < MaxSeq /\ ParentComplete(s)

Init ==
    /\ owner      = [s \in Shards |-> NONE]
    /\ inflight   = [w \in Workers |-> {}]
    /\ cap        = [w \in Workers |-> 1]
    /\ checkpoint = [s \in Shards |-> 0]
    /\ delivered  = [s \in Shards |-> 0]
    /\ crashes    = 0
    /\ handoffs   = 0

(* Take the lease on an unowned, eligible shard. *)
AcquireLease(w, s) ==
    /\ owner[s] = NONE
    /\ Eligible(s)
    /\ owner' = [owner EXCEPT ![s] = w]
    /\ UNCHANGED <<inflight, cap, checkpoint, delivered, crashes, handoffs>>

(* Acquire a processing slot for an owned shard: this is where the cap      *)
(* binds (Cardinality(inflight[w]) < cap[w]).                                *)
StartProcess(w, s) ==
    /\ owner[s] = w
    /\ s \notin inflight[w]
    /\ checkpoint[s] < MaxSeq
    /\ Cardinality(inflight[w]) < cap[w]
    /\ inflight' = [inflight EXCEPT ![w] = @ \cup {s}]
    /\ UNCHANGED <<owner, cap, checkpoint, delivered, crashes, handoffs>>

(* Deliver the in-flight record, advance the durable checkpoint by one,     *)
(* release the slot.                                                        *)
Complete(w, s) ==
    /\ s \in inflight[w]
    /\ delivered'  = [delivered  EXCEPT ![s] = @ + 1]
    /\ checkpoint' = [checkpoint EXCEPT ![s] = @ + 1]
    /\ inflight'   = [inflight EXCEPT ![w] = @ \ {s}]
    /\ UNCHANGED <<owner, cap, crashes, handoffs>>

(* Crash while processing: record may have been delivered (at-least-once),  *)
(* checkpoint NOT advanced, slot freed and lease dropped -> reprocessed.    *)
Crash(w, s) ==
    /\ s \in inflight[w]
    /\ crashes < MaxCrashes
    /\ delivered' = [delivered EXCEPT ![s] = @ + 1]
    /\ inflight'  = [inflight EXCEPT ![w] = @ \ {s}]
    /\ owner'     = [owner EXCEPT ![s] = NONE]
    /\ crashes'   = crashes + 1
    /\ UNCHANGED <<cap, checkpoint, handoffs>>

(* Give up a lease at a batch boundary (steal/expiry): only when not        *)
(* mid-process, so another worker can pick the shard up. Bounded.           *)
LoseLease(w, s) ==
    /\ owner[s] = w
    /\ s \notin inflight[w]
    /\ handoffs < MaxHandoffs
    /\ owner' = [owner EXCEPT ![s] = NONE]
    /\ handoffs' = handoffs + 1
    /\ UNCHANGED <<inflight, cap, checkpoint, delivered, crashes>>

(* Online resize of a worker's cap. Grow freely; shrink only to >= the      *)
(* current in-flight count (the "shrink waits for a slot to free" rule),    *)
(* so PerWorkerBound is preserved across the resize. k /= current => a real *)
(* transition.                                                              *)
Resize(w) ==
    /\ \E k \in 1..MaxCap :
        /\ k # cap[w]
        /\ k >= Cardinality(inflight[w])
        /\ cap' = [cap EXCEPT ![w] = k]
    /\ UNCHANGED <<owner, inflight, checkpoint, delivered, crashes, handoffs>>

Next ==
    \/ \E w \in Workers, s \in Shards :
          \/ AcquireLease(w, s)
          \/ StartProcess(w, s)
          \/ Complete(w, s)
          \/ Crash(w, s)
          \/ LoseLease(w, s)
    \/ \E w \in Workers : Resize(w)

(* Fairness: some worker eventually leases each eligible shard, and the     *)
(* owner eventually starts + completes it. Crashes and handoffs are bounded *)
(* (finite disruption), so the system drains. Resize is left unfair (it     *)
(* must not be required for progress).                                      *)
AcquireSome(s) == \E w \in Workers : AcquireLease(w, s)
Fairness ==
    /\ \A s \in Shards : SF_vars(AcquireSome(s))
    /\ \A w \in Workers, s \in Shards : SF_vars(StartProcess(w, s))
    /\ \A w \in Workers, s \in Shards : SF_vars(Complete(w, s))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)
PerWorkerBound  == \A w \in Workers : Cardinality(inflight[w]) <= cap[w]
MutualExclusion == \A w1, w2 \in Workers :
                       (w1 # w2) => (inflight[w1] \cap inflight[w2] = {})
OwnedWhileProc  == \A w \in Workers : \A s \in inflight[w] : owner[s] = w
ParentBeforeChild ==
    \A pr \in ParentOf :
        LET c == pr[1] p == pr[2] IN
        (checkpoint[c] > 0 \/ (\E w \in Workers : c \in inflight[w]))
            => checkpoint[p] = MaxSeq
CheckpointOK == \A s \in Shards : checkpoint[s] \in 0..MaxSeq
AtLeastOnce  == \A s \in Shards : delivered[s] >= checkpoint[s]

(* Liveness *)
Done        == \A s \in Shards : checkpoint[s] = MaxSeq
Termination == <>Done
=============================================================================
