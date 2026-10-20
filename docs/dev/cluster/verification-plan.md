# Cluster verification plan

- **Status:** proposed
- **Date:** 2026-10-20
- **Related:** [ADR-0001](../adr/0001-cluster-raft-controller-and-per-repository-primaries.md)

This plan describes how the clustered Gogs of ADR-0001 is verified against its [safety rules](../adr/0001-cluster-raft-controller-and-per-repository-primaries.md#safety-rules), and which checks gate each phase of the ADR.

## Approach

Verification happens at three levels, each catching a different class of bugs:

| Level | Tool | Verifies | Available |
|---|---|---|---|
| Design | TLA+ model checked with TLC | that the protocol is safe in every interleaving of steps and faults, within small bounds | now, before any code |
| Implementation | Go unit and integration tests | the parts that a protocol model cannot express: determinism, idempotency, atomic apply | from phase 1 |
| System | Jepsen | that the running cluster respects the safety rules under real faults | from phase 1 (single node), fully from phase 3 |

A passing TLA+ model does not prove that the code is correct, and a passing Jepsen run does not prove that bugs are absent. Together they cover the design and a large sample of the real behavior.

## Design level: TLA+

The model lives in [`tla/`](tla/README.md). It covers one repository with a primary, pull replicas, the ISR, the high watermark (HW), epochs, crashes, disk replacements, zombie primaries and planned primary moves. The controller is modeled as an atomic store, relying on the existing Raft specification.

Each safety rule that the model covers has an invariant, and a mutation model that removes the rule. The main model must pass, and every mutation model must fail with its expected violation. A mutation that does not fail means the model is too weak to detect that bug.

The model is checked at two moments:

- Before phase 2 starts, as the design gate.
- Again whenever the implementation deviates from the model, e.g., in the truncation exchange after a failover.

## System level: Jepsen

### Environment

- A control node and 5 Linux nodes, e.g., with the Docker setup that ships with Jepsen. Five nodes allow a controller of 3 voters plus replicas placed on different nodes.
- The Gogs binary built from the branch under test, installed and configured by the test over SSH.
- The `git` CLI on the control node. Clients talk to random nodes, so the forwarding path to the primary is always exercised.
- [lazyfs](https://github.com/dsrhaslab/lazyfs) under the data directories, to simulate the loss of writes that were not fsynced.

### Client operations

The Jepsen client drives Git through its CLI, the same way real users do:

| Operation | Git command | Result |
|---|---|---|
| Read a ref | `git ls-remote <url> refs/heads/<key>` | the commit, or nil |
| Write a ref | `git push --force <url> <commit>:refs/heads/<key>` | ok, fail or info |
| Compare-and-swap a ref | `git push --force-with-lease=refs/heads/<key>:<expected> <url> <commit>:refs/heads/<key>` | ok if the ref still had the expected value |
| Append to a branch | commit on top of the current tip, then push without force | ok, or fail if the tip moved |
| Read a branch | `git fetch` then `git log --first-parent --format=%s` | the list of appended values |

Every value is encoded in the commit message, so each commit hash maps back to one value. A timeout or a network error is recorded as indeterminate (`:info`), because the push may have been committed anyway.

Reads run in two modes. Reads routed to the primary must be linearizable. Reads from any node may be stale by design, so they are checked against a weaker model.

### Workloads

1. **Ref register.** Independent keys, each a ref under read, write and compare-and-swap. Checked with Knossos against a compare-and-swap register model, with reads routed to the primary.
2. **Branch append.** Each key is a branch, and each operation appends a commit or reads the whole history. Checked with Elle's list-append checker: strict serializable with reads from the primary, serializable with reads from any node. Elle reports lost appends, duplicates, and reads that a later read does not extend (`incompatible-order`).
3. **Clone and fsck.** Periodically, and once more after all faults are healed, clone from every node and run `git fsck --full`. Every ref must resolve to complete objects, every acknowledged append must be present, and all ISR members must end with identical refs.
4. **Global set.** Create users and teams through the API and read them back. Checked with Jepsen's set checker: every acknowledged element must be present in the final read.
5. **Repository data.** Comments on an issue as a list-append workload through the API. After healing, the rows of the test repositories are dumped from every node and compared: all ISR members must hold identical rows up to the same HW.
6. **Webhooks.** A receiver on the control node records every delivery. Every acknowledged push must be delivered at least once, all deliveries of a push must carry the same idempotency key, and no push that failed definitively may be delivered.

### Nemeses

Faults are injected with Jepsen's combined nemesis package. The Gogs DB adapter implements process control, pause and primary discovery, so faults can target primaries specifically.

| Nemesis | What it simulates |
|---|---|
| Partitions: random halves, majorities ring, isolate the primary, isolate the controller leader | network splits, including the zombie primary |
| Kill (`SIGKILL`) and restart, on random nodes, primaries and the controller leader | crashes |
| Pause (`SIGSTOP` and `SIGCONT`) longer than the session timeout | a stalled primary that wakes up after a failover |
| lazyfs, losing unfsynced writes on kill | power loss, and the durability claims of Pebble and SQLite |
| Disk wipe of a stopped node | disk replacement |
| Clock skew | wrong clocks on the paths that use timeouts |
| Packet delay and loss | WAN links between sites, slow replicas leaving the ISR |
| Membership: add and remove nodes, planned primary move | operations on a live cluster |

### Coverage of the safety rules

| Rule | TLA+ | Jepsen workload | Nemesis | Checker | Phase |
|---|---|---|---|---|---|
| 1. Clean election | `CommittedOnIsr`, `MC_UncleanElection` | ref register, branch append | kill, isolate and pause the primary | Knossos, Elle | 3, 4 |
| 2. ISR managed by the controller | `CommittedOnIsr`, `MC_LocalIsrShrink` | branch append | packet delay on a replica, then kill the primary | Elle | 3, 4 |
| 3. Minimum ISR | `CommittedDurable`, `MC_MinIsrOne` | branch append, clone and fsck | kill two replicas, disk wipe | Elle, fsck. No write may be acknowledged while fewer than `MinIsr` replicas are in sync. | 4 |
| 4. Epoch on every message | fencing guards of `Fetch` and `Truncate` | ref register | pause the primary, isolate it from the controller only | Knossos | 4 |
| 5. Objects before refs | `NoDanglingRefs`, `MC_RefsBeforeObjects` | clone and fsck | kill during pushes, partition between primary and replicas | fsck | 3 |
| 6. No exposure beyond the HW | `ExposedIsCommitted`, `MC_ExposeLeo` | branch append, reads from any node | partitions, kill | Elle (`incompatible-order`, lost appends) | 3 |
| 7. Side effects exactly once | not modeled | webhooks | kill the primary right after a push | webhook checker | 5 |
| 8. Determinism | not modeled | repository data | restarts | row comparison across nodes | 5 |
| 9. Atomic apply | not modeled | repository data | kill during apply, lazyfs | Elle (duplicates, lost comments), row comparison | 5 |

### Pass criteria

- Every combination of workload and nemesis planned for a phase runs at least 20 times, for at least 10 minutes each, with every checker reporting `:valid? true`.
- A result of `:valid? :unknown` counts as a failure until it is explained.
- Every failure is reproduced, reduced to a minimal history, fixed, and kept as a regression run. If the cause is in the protocol, the TLA+ model is updated first, and a model that does not detect the bug counts as a gap in the model.
- Availability is measured but is not a pass criterion, with one exception: writes must be rejected, not acknowledged, while the ISR is below its minimum.

### Proposed layout

```text
test/jepsen/
  project.clj
  src/jepsen/gogs.clj           test entry point and CLI
  src/jepsen/gogs/db.clj        install, configure, start, kill, pause, primary discovery
  src/jepsen/gogs/git.clj       Git CLI client
  src/jepsen/gogs/workload/     register, append, fsck, set, issues, webhooks
  src/jepsen/gogs/nemesis.clj   Gogs-specific faults (isolate primary, disk wipe)
```

The suite is a separate Clojure project, so the Gogs build does not depend on it.

## Gates by phase

| ADR phase | Gate |
|---|---|
| 1. Single-node preparation | TLC passes on `MC.cfg` and finds the expected violation in every mutation model. The Jepsen suite runs against a single node with the ref register, branch append and clone and fsck workloads, under kill and lazyfs, as a baseline that validates the harness. |
| 2. Controller | Global set workload, with kill and partitions targeting the controller leader. |
| 3. Per-repository log | Ref register, branch append and clone and fsck, with partitions, kill, pause and lazyfs. |
| 4. Failover and operations | Phase 3 suites plus membership changes, planned primary moves, disk wipes and clock skew. The TLA+ model is updated if the implemented truncation exchange differs from the model. |
| 5. Repository data | Repository data and webhook workloads, with kill during apply and lazyfs. |
| 6. Fault tests and geography | The full matrix with packet delay between sites and observer replicas, run nightly. |

## References

- TLA+: <https://lamport.azurewebsites.net/tla/tla.html>.
- Raft TLA+ specification: <https://github.com/ongardie/raft.tla>.
- Jepsen: <https://github.com/jepsen-io/jepsen>.
- Elle, the transactional consistency checker: <https://github.com/jepsen-io/elle>.
- Knossos, the linearizability checker: <https://github.com/jepsen-io/knossos>.
- lazyfs: <https://github.com/dsrhaslab/lazyfs>.
