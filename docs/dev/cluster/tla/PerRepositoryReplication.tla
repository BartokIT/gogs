--------------------------- MODULE PerRepositoryReplication ---------------------------
(***************************************************************************)
(* Model of the per-repository replication protocol of ADR-0001:           *)
(* a primary, pull replicas, an ISR managed by the controller, a high      *)
(* watermark (HW) and epochs for fencing.                                  *)
(*                                                                         *)
(* Scope and abstractions:                                                 *)
(*   - One repository. Repositories are independent in the design.         *)
(*   - The controller is a Raft group. It is modeled as an atomic,         *)
(*     linearizable store (ctrlEpoch, ctrlPrimary, ctrlIsr), relying on    *)
(*     the existing Raft TLA+ specification for its own correctness.       *)
(*   - A log entry is a ref update. Its value also names the Git objects   *)
(*     it needs, so "having the objects" means the value is in objs[n].    *)
(*   - The network can delay any message forever: a step that needs two   *)
(*     nodes may simply never happen. This also models partitions and the  *)
(*     zombie primary, i.e., a primary that is alive but cut off.          *)
(*   - Logs survive crashes (they are fsynced). LoseDisk models replacing  *)
(*     a node's disk, which wipes its state.                               *)
(*                                                                         *)
(* The Unsafe* constants are mutations. Each one removes a safety rule of  *)
(* the ADR, and TLC is expected to find a violation when it is TRUE.       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Nodes,                   \* Nodes hosting a replica of the repository.
    Values,                  \* Pushes. Each value is written at most once.
    MinIsr,                  \* Minimum ISR size to commit (safety rule 3).
    MaxCrashes,              \* Bound on process crashes.
    MaxDiskLosses,           \* Bound on disk replacements.
    MaxEpoch,                \* Bound on primary elections.
    UnsafeLocalIsrShrink,    \* Mutation of safety rule 2.
    UncleanElection,         \* Mutation of safety rule 1.
    UnsafeRefsBeforeObjects, \* Mutation of safety rule 5.
    UnsafeExposeLeo          \* Mutation of safety rule 6.

ASSUME
    /\ MinIsr \in 1..Cardinality(Nodes)
    /\ MaxCrashes \in Nat
    /\ MaxDiskLosses \in Nat
    /\ MaxEpoch \in Nat \ {0}
    /\ UnsafeLocalIsrShrink \in BOOLEAN
    /\ UncleanElection \in BOOLEAN
    /\ UnsafeRefsBeforeObjects \in BOOLEAN
    /\ UnsafeExposeLeo \in BOOLEAN

VARIABLES
    \* Faults.
    alive,           \* Nodes whose process is running.
    crashes,         \* Number of crashes so far.
    diskLosses,      \* Number of disk replacements so far.
    \* Controller state, replicated by Raft.
    ctrlEpoch,       \* Current epoch of the repository.
    ctrlPrimary,     \* Primary appointed for ctrlEpoch.
    ctrlIsr,         \* Authoritative ISR.
    \* Per-node state.
    log,             \* log[n]: the node's replication log.
    objs,            \* objs[n]: values whose Git objects the node has.
    hw,              \* hw[n]: the node's HW. Refs are exposed up to it.
    nodeEpoch,       \* nodeEpoch[n]: the latest epoch known by the node.
    believesPrimary, \* Nodes that currently act as primary.
    isrView,         \* isrView[p]: the ISR as known by p when acting as primary.
    ackedUpTo,       \* ackedUpTo[p][r]: log length that r reported to p.
    epochStart,      \* epochStart[p]: log length of p when it became primary.
    \* Ghost state, used only by the invariants.
    used,            \* Values already written by a client.
    committed        \* Entries acknowledged to clients, in order.

faultVars == <<alive, crashes, diskLosses>>
ctrlVars == <<ctrlEpoch, ctrlPrimary, ctrlIsr>>
nodeVars == <<log, objs, hw, nodeEpoch, believesPrimary, isrView, ackedUpTo, epochStart>>
ghostVars == <<used, committed>>
vars == <<faultVars, ctrlVars, nodeVars, ghostVars>>

-----------------------------------------------------------------------------
(* Helpers. *)

Entry == [epoch : 1..MaxEpoch, val : Values]

MaxLen == Cardinality(Values)

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y

IsPrefix(s, t) == Len(s) <= Len(t) /\ SubSeq(t, 1, Len(s)) = s

\* Length of the longest common prefix of s and t. The implementation
\* computes it with the epoch-based exchange of Kafka's KIP-101 and KIP-279.
CommonPrefixLen(s, t) ==
    CHOOSE k \in 0..Min(Len(s), Len(t)) :
        /\ SubSeq(s, 1, k) = SubSeq(t, 1, k)
        /\ \/ k = Min(Len(s), Len(t))
           \/ s[k + 1] # t[k + 1]

FirstPrimary == CHOOSE n \in Nodes : TRUE

-----------------------------------------------------------------------------
(* Initial state: every node is in the ISR, FirstPrimary leads epoch 1. *)

Init ==
    /\ alive = Nodes
    /\ crashes = 0
    /\ diskLosses = 0
    /\ ctrlEpoch = 1
    /\ ctrlPrimary = FirstPrimary
    /\ ctrlIsr = Nodes
    /\ log = [n \in Nodes |-> <<>>]
    /\ objs = [n \in Nodes |-> {}]
    /\ hw = [n \in Nodes |-> 0]
    /\ nodeEpoch = [n \in Nodes |-> 1]
    /\ believesPrimary = {FirstPrimary}
    /\ isrView = [n \in Nodes |-> Nodes]
    /\ ackedUpTo = [p \in Nodes |-> [r \in Nodes |-> 0]]
    /\ epochStart = [n \in Nodes |-> 0]
    /\ used = {}
    /\ committed = <<>>

-----------------------------------------------------------------------------
(* Write path. *)

\* A client push reaches a node that acts as primary. The pack is received
\* in quarantine, so the primary has the objects before the entry exists.
ClientWrite(p, v) ==
    /\ p \in alive
    /\ p \in believesPrimary
    /\ v \in Values \ used
    /\ log' = [log EXCEPT ![p] = Append(@, [epoch |-> nodeEpoch[p], val |-> v])]
    /\ objs' = [objs EXCEPT ![p] = @ \cup {v}]
    /\ used' = used \cup {v}
    /\ UNCHANGED <<faultVars, ctrlVars, hw, nodeEpoch, believesPrimary,
                   isrView, ackedUpTo, epochStart, committed>>

\* A replica copies Git objects from another node (git fetch). Objects are
\* verified by hash, so any node is a valid source and no epoch is needed.
FetchObjects(r, p) ==
    /\ r \in alive
    /\ p \in alive
    /\ r # p
    /\ ~(objs[p] \subseteq objs[r])
    /\ objs' = [objs EXCEPT ![r] = @ \cup objs[p]]
    /\ UNCHANGED <<faultVars, ctrlVars, log, hw, nodeEpoch, believesPrimary,
                   isrView, ackedUpTo, epochStart, ghostVars>>

\* A replica pulls the next log entry from a primary of the same epoch
\* (safety rule 4), reports its log length and learns the HW. Safety rule 5:
\* the entry is appended only if the replica already has its objects.
\* Safety rule 6: the replica exposes refs only up to the primary's HW.
Fetch(r, p) ==
    /\ r \in alive
    /\ p \in alive
    /\ r # p
    /\ p \in believesPrimary
    /\ r \notin believesPrimary
    /\ nodeEpoch[r] = nodeEpoch[p]
    /\ IsPrefix(log[r], log[p])
    /\ LET next == Len(log[r]) + 1
           canAppend == /\ next <= Len(log[p])
                        /\ \/ UnsafeRefsBeforeObjects
                           \/ log[p][next].val \in objs[r]
           newLog == IF canAppend THEN Append(log[r], log[p][next]) ELSE log[r]
       IN  /\ log' = [log EXCEPT ![r] = newLog]
           /\ ackedUpTo' = [ackedUpTo EXCEPT ![p][r] = Len(newLog)]
           /\ hw' = [hw EXCEPT ![r] =
                        IF UnsafeExposeLeo
                        THEN Len(newLog)
                        ELSE Max(@, Min(hw[p], Len(newLog)))]
    /\ UNCHANGED <<faultVars, ctrlVars, objs, nodeEpoch, believesPrimary,
                   isrView, epochStart, ghostVars>>

\* A replica whose log diverges from the primary of its epoch truncates it
\* to the common prefix and resets its refs accordingly.
Truncate(r, p) ==
    /\ r \in alive
    /\ p \in alive
    /\ r # p
    /\ p \in believesPrimary
    /\ r \notin believesPrimary
    /\ nodeEpoch[r] = nodeEpoch[p]
    /\ ~IsPrefix(log[r], log[p])
    /\ LET k == CommonPrefixLen(log[r], log[p])
       IN  /\ log' = [log EXCEPT ![r] = SubSeq(@, 1, k)]
           /\ hw' = [hw EXCEPT ![r] = Min(@, k)]
    /\ UNCHANGED <<faultVars, ctrlVars, objs, nodeEpoch, believesPrimary,
                   isrView, ackedUpTo, epochStart, ghostVars>>

\* The primary advances its HW to the shortest log among the ISR members it
\* knows (safety rule 3: only with at least MinIsr members). Entries up to
\* the new HW are acknowledged to clients.
AdvanceHW(p) ==
    /\ p \in alive
    /\ p \in believesPrimary
    /\ Cardinality(isrView[p]) >= MinIsr
    /\ LET acks == {ackedUpTo[p][m] : m \in isrView[p] \ {p}} \cup {Len(log[p])}
           newHW == SetMin(acks)
       IN  /\ newHW > hw[p]
           /\ hw' = [hw EXCEPT ![p] = newHW]
           /\ committed' = IF newHW > Len(committed)
                           THEN SubSeq(log[p], 1, newHW)
                           ELSE committed
    /\ UNCHANGED <<faultVars, ctrlVars, log, objs, nodeEpoch, believesPrimary,
                   isrView, ackedUpTo, epochStart, used>>

-----------------------------------------------------------------------------
(* ISR changes. *)

\* Safety rule 2: the current primary asks the controller to remove a
\* replica, and the change takes effect only once the controller records it.
ShrinkIsr(p, m) ==
    /\ p \in alive
    /\ p \in believesPrimary
    /\ p = ctrlPrimary
    /\ nodeEpoch[p] = ctrlEpoch
    /\ m \in ctrlIsr
    /\ m # p
    /\ ctrlIsr' = ctrlIsr \ {m}
    /\ isrView' = [isrView EXCEPT ![p] = ctrlIsr \ {m}]
    /\ UNCHANGED <<faultVars, ctrlEpoch, ctrlPrimary, log, objs, hw, nodeEpoch,
                   believesPrimary, ackedUpTo, epochStart, ghostVars>>

\* Mutation of safety rule 2: a primary shrinks its own view of the ISR
\* without the controller.
LocalShrinkIsr(p, m) ==
    /\ UnsafeLocalIsrShrink
    /\ p \in alive
    /\ p \in believesPrimary
    /\ m \in isrView[p]
    /\ m # p
    /\ isrView' = [isrView EXCEPT ![p] = @ \ {m}]
    /\ UNCHANGED <<faultVars, ctrlVars, log, objs, hw, nodeEpoch, believesPrimary,
                   ackedUpTo, epochStart, ghostVars>>

\* The current primary adds back a replica that has caught up both to the HW
\* and to the start of the current epoch, as Kafka requires.
ExpandIsr(p, m) ==
    /\ p \in alive
    /\ p \in believesPrimary
    /\ p = ctrlPrimary
    /\ nodeEpoch[p] = ctrlEpoch
    /\ m \notin ctrlIsr
    /\ ackedUpTo[p][m] >= Max(hw[p], epochStart[p])
    /\ ctrlIsr' = ctrlIsr \cup {m}
    /\ isrView' = [isrView EXCEPT ![p] = ctrlIsr \cup {m}]
    /\ UNCHANGED <<faultVars, ctrlEpoch, ctrlPrimary, log, objs, hw, nodeEpoch,
                   believesPrimary, ackedUpTo, epochStart, ghostVars>>

-----------------------------------------------------------------------------
(* Failover and fencing. *)

\* The controller appoints a new primary and increments the epoch. It may do
\* so at any time: after a real failure, after a false failure detection
\* (the old primary becomes a zombie), or for a planned move. Safety rule 1:
\* the new primary is an ISR member, unless UncleanElection is TRUE.
ElectPrimary(n) ==
    /\ ctrlEpoch < MaxEpoch
    /\ n \in alive
    /\ n # ctrlPrimary
    /\ \/ n \in ctrlIsr
       \/ UncleanElection
    /\ LET newIsr == IF n \in ctrlIsr THEN ctrlIsr ELSE {n}
       IN  /\ ctrlEpoch' = ctrlEpoch + 1
           /\ ctrlPrimary' = n
           /\ ctrlIsr' = newIsr
           /\ nodeEpoch' = [nodeEpoch EXCEPT ![n] = ctrlEpoch + 1]
           /\ believesPrimary' = believesPrimary \cup {n}
           /\ isrView' = [isrView EXCEPT ![n] = newIsr]
           /\ ackedUpTo' = [ackedUpTo EXCEPT ![n] = [r \in Nodes |-> 0]]
           /\ epochStart' = [epochStart EXCEPT ![n] = Len(log[n])]
    /\ UNCHANGED <<faultVars, log, objs, hw, ghostVars>>

\* A node learns the current epoch from the controller. A former primary
\* stops acting as primary from this moment.
LearnEpoch(m) ==
    /\ m \in alive
    /\ nodeEpoch[m] < ctrlEpoch
    /\ nodeEpoch' = [nodeEpoch EXCEPT ![m] = ctrlEpoch]
    /\ believesPrimary' = believesPrimary \ {m}
    /\ UNCHANGED <<faultVars, ctrlVars, log, objs, hw, isrView, ackedUpTo,
                   epochStart, ghostVars>>

-----------------------------------------------------------------------------
(* Faults. *)

Crash(n) ==
    /\ n \in alive
    /\ crashes < MaxCrashes
    /\ alive' = alive \ {n}
    /\ crashes' = crashes + 1
    /\ believesPrimary' = believesPrimary \ {n}
    /\ UNCHANGED <<diskLosses, ctrlVars, log, objs, hw, nodeEpoch, isrView,
                   ackedUpTo, epochStart, ghostVars>>

\* A restarted node resumes as primary only if the controller still lists it
\* as primary of the current epoch and as an ISR member.
Restart(n) ==
    /\ n \notin alive
    /\ alive' = alive \cup {n}
    /\ IF /\ n = ctrlPrimary
          /\ n \in ctrlIsr
          /\ nodeEpoch[n] = ctrlEpoch
       THEN /\ believesPrimary' = believesPrimary \cup {n}
            /\ isrView' = [isrView EXCEPT ![n] = ctrlIsr]
            /\ ackedUpTo' = [ackedUpTo EXCEPT ![n] = [r \in Nodes |-> 0]]
       ELSE UNCHANGED <<believesPrimary, isrView, ackedUpTo>>
    /\ UNCHANGED <<crashes, diskLosses, ctrlVars, log, objs, hw, nodeEpoch,
                   epochStart, ghostVars>>

\* The disk of a stopped node is replaced. The controller removes the node
\* from the ISR and tells the current primary, which stops waiting for it.
LoseDisk(n) ==
    /\ n \notin alive
    /\ diskLosses < MaxDiskLosses
    /\ diskLosses' = diskLosses + 1
    /\ log' = [log EXCEPT ![n] = <<>>]
    /\ objs' = [objs EXCEPT ![n] = {}]
    /\ hw' = [hw EXCEPT ![n] = 0]
    /\ nodeEpoch' = [nodeEpoch EXCEPT ![n] = 0]
    /\ ctrlIsr' = ctrlIsr \ {n}
    /\ isrView' = [isrView EXCEPT ![ctrlPrimary] = @ \ {n}]
    /\ ackedUpTo' = [p \in Nodes |-> [ackedUpTo[p] EXCEPT ![n] = 0]]
    /\ UNCHANGED <<alive, crashes, ctrlEpoch, ctrlPrimary, believesPrimary,
                   epochStart, ghostVars>>

-----------------------------------------------------------------------------
(* Specification. *)

Next ==
    \/ \E p \in Nodes, v \in Values : ClientWrite(p, v)
    \/ \E r, p \in Nodes : FetchObjects(r, p) \/ Fetch(r, p) \/ Truncate(r, p)
    \/ \E p \in Nodes : AdvanceHW(p)
    \/ \E p, m \in Nodes : ShrinkIsr(p, m) \/ LocalShrinkIsr(p, m) \/ ExpandIsr(p, m)
    \/ \E n \in Nodes : ElectPrimary(n) \/ LearnEpoch(n)
    \/ \E n \in Nodes : Crash(n) \/ Restart(n) \/ LoseDisk(n)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants. The comments name the safety rules of ADR-0001. *)

TypeOK ==
    /\ alive \subseteq Nodes
    /\ crashes \in 0..MaxCrashes
    /\ diskLosses \in 0..MaxDiskLosses
    /\ ctrlEpoch \in 1..MaxEpoch
    /\ ctrlPrimary \in Nodes
    /\ ctrlIsr \subseteq Nodes
    /\ log \in [Nodes -> Seq(Entry)]
    /\ \A n \in Nodes : Len(log[n]) <= MaxLen
    /\ objs \in [Nodes -> SUBSET Values]
    /\ hw \in [Nodes -> 0..MaxLen]
    /\ nodeEpoch \in [Nodes -> 0..MaxEpoch]
    /\ believesPrimary \subseteq Nodes
    /\ isrView \in [Nodes -> SUBSET Nodes]
    /\ ackedUpTo \in [Nodes -> [Nodes -> 0..MaxLen]]
    /\ epochStart \in [Nodes -> 0..MaxLen]
    /\ used \subseteq Values
    /\ committed \in Seq(Entry)

\* Refs are never exposed beyond the end of the local log.
HwWithinLog ==
    \A n \in Nodes : hw[n] <= Len(log[n])

\* Rules 1 and 2: every member of the authoritative ISR holds every
\* acknowledged entry, so any electable primary has them.
CommittedOnIsr ==
    \A n \in ctrlIsr : IsPrefix(committed, log[n])

\* Rule 3: acknowledged entries survive the allowed disk losses.
CommittedDurable ==
    \E n \in Nodes : IsPrefix(committed, log[n])

\* Rule 5: no exposed ref points to missing objects.
NoDanglingRefs ==
    \A n \in Nodes : \A i \in 1..Min(hw[n], Len(log[n])) : log[n][i].val \in objs[n]

\* Rule 6: what a node exposes has been acknowledged and is never rewritten.
ExposedIsCommitted ==
    \A n \in Nodes : IsPrefix(SubSeq(log[n], 1, Min(hw[n], Len(log[n]))), committed)

\* Acknowledged entries are never lost or rewritten.
CommittedMonotonic ==
    [][IsPrefix(committed, committed')]_committed

=============================================================================
