# TLA+ model of per-repository replication

[`PerRepositoryReplication.tla`](PerRepositoryReplication.tla) models the replication protocol of [ADR-0001](../../adr/0001-cluster-raft-controller-and-per-repository-primaries.md): a primary, pull replicas, an ISR managed by the controller, a high watermark and epochs for fencing. The invariants are the safety rules of the ADR. The [verification plan](../verification-plan.md) explains how this model fits with the Jepsen suite.

## Models

Every model uses 3 nodes, 2 pushes, up to 2 crashes, 1 disk replacement and 3 epochs.

| Config | Change | Expected result |
|---|---|---|
| [`MC.cfg`](MC.cfg) | none | no violation |
| [`MC_LocalIsrShrink.cfg`](MC_LocalIsrShrink.cfg) | the primary shrinks its ISR without the controller (breaks rule 2) | `CommittedOnIsr` violated |
| [`MC_UncleanElection.cfg`](MC_UncleanElection.cfg) | a replica outside the ISR can be elected (breaks rule 1) | `CommittedOnIsr` violated |
| [`MC_MinIsrOne.cfg`](MC_MinIsrOne.cfg) | `MinIsr = 1` (breaks rule 3) | `CommittedDurable` violated |
| [`MC_RefsBeforeObjects.cfg`](MC_RefsBeforeObjects.cfg) | replicas accept entries before their objects (breaks rule 5) | `NoDanglingRefs` violated |
| [`MC_ExposeLeo.cfg`](MC_ExposeLeo.cfg) | replicas expose refs up to their log end (breaks rule 6) | `ExposedIsCommitted` violated |

The mutation models check the model itself: if a mutation does not produce its expected violation, the invariant or the model is too weak to catch that bug.

## Running TLC

TLC needs Java 11 or later. The simplest option is the [TLA+ extension for VS Code](https://github.com/tlaplus/vscode-tlaplus), which bundles the tools: open the `.tla` file and run "TLA+: Check model with TLC" with the chosen config.

From the command line, with [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases):

```bash
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -workers auto -config MC.cfg PerRepositoryReplication.tla
```

Run the same command with each `MC_*.cfg` to check the mutations. TLC prints the shortest trace that leads to each violation.

## Status

Checking `MC.cfg` and every mutation model is the design gate of phase 1 in the ADR. Any violation found in `MC.cfg` must be resolved in the model and in the ADR before phase 2 starts.

Last run: 2026-10-20, TLC 2.19 (`tla2tools.jar` v1.7.4) on Java 21, 16 cores, with the bounds listed above. SANY parses the module without errors. Every model gives its expected result, so the gate is passed.

| Config | Result | Distinct states | Depth | Time |
|---|---|---|---|---|
| `MC.cfg` | no violation of the invariants or of `CommittedMonotonic` | 70,749,618 | 34 | 3 min 12 s (12 workers) |
| `MC_LocalIsrShrink.cfg` | `CommittedOnIsr` violated | 2,530 | 6 | under 1 s |
| `MC_UncleanElection.cfg` | `CommittedOnIsr` violated | 7,733 | 8 | under 1 s |
| `MC_MinIsrOne.cfg` | `CommittedDurable` violated | 15,704 | 7 | under 1 s |
| `MC_RefsBeforeObjects.cfg` | `NoDanglingRefs` violated | 2,736 | 7 | under 1 s |
| `MC_ExposeLeo.cfg` | `ExposedIsCommitted` violated | 60 | 4 | under 1 s |

For the mutations, the counts are the states explored before TLC stopped at the first violation, and the depth is the length of the trace. Each trace shows the intended bug:

- `MC_LocalIsrShrink`: the primary writes, one replica copies the entry, the primary drops the other replica from its own ISR view and commits. The controller still lists the lagging replica in the ISR.
- `MC_UncleanElection`: the controller removes a lagging replica from the ISR, the primary commits, then the lagging replica is elected and becomes the only ISR member.
- `MC_MinIsrOne`: the controller shrinks the ISR to the primary alone, the primary commits, crashes and loses its disk.
- `MC_RefsBeforeObjects`: a replica appends an entry without its objects, then exposes it once the primary advances the HW.
- `MC_ExposeLeo`: a replica exposes an entry right after copying it, before the primary commits it.

TLC estimates the probability of a fingerprint collision in `MC.cfg` at about 1E-4. The run of `MC.cfg` writes about 1.5 GB to disk for the state queue and the fingerprint set.

## Not covered

- The internals of Raft. The controller is modeled as an atomic store, relying on the existing [Raft TLA+ specification](https://github.com/ongardie/raft.tla).
- Liveness, i.e., that writes eventually succeed when enough nodes are healthy.
- Safety rules 7, 8 and 9 (side effects, determinism, atomic apply). They concern the implementation and are covered by the Jepsen suite and by unit tests.
- Loss of fsynced data other than a full disk replacement.
