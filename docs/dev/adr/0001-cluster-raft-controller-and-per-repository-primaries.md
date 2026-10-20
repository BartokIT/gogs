# ADR-0001: Gogs cluster without external dependencies, with a Raft controller and per-repository primaries

- **Status:** proposed
- **Date:** 2026-10-19
- **Last updated:** 2026-10-20, choice of local storage (Pebble for logs, SQLite for state)
- **Deciders:** BartokIT

## Context

Gogs today is a single process. Repositories are bare repositories on the local disk (`[repository] ROOT`, with paths computed in [`internal/repox/repox.go`](../../../internal/repox/repox.go)), metadata lives in a database (SQLite, PostgreSQL or MySQL), and sessions and cache live in memory (`[session] PROVIDER = memory` and `[cache] ADAPTER = memory` in [`conf/app.ini`](../../../conf/app.ini)). High availability is only possible today with external infrastructure: a managed database and shared storage.

The goal is a distributed Git server with these properties:

1. At least 3 nodes, possibly in geographically distant sites.
2. A single binary: no external database, no object storage (e.g., S3), no distributed filesystem, no external coordinator.
3. A push acknowledged to the client is never lost, and `main` can never diverge between nodes.
4. The cluster survives the loss of a node or of an entire site.
5. Reads (clone, fetch, web pages) are served by the nearest node.
6. The system scales with the number of repositories, which is the dimension that grows in a forge.

The state of a forge splits into two parts with opposite problems:

| Part | Examples | Size | Problem |
|---|---|---|---|
| Mutable state | refs (where `main` points), users, permissions, issues, PRs | small | must be ordered: two concurrent writes need a decision shared by all nodes, i.e., consensus |
| Immutable data | Git objects, LFS objects, attachments | large | no conflicts are possible: the name is the hash of the content, so it can be copied from any node and verified locally |

Git already provides an efficient and verifiable protocol to replicate the second part: `git fetch`. The first part requires a consensus protocol embedded in the binary.

## Decision

We adopt a two-level architecture, modeled on Kafka's replication protocol with KRaft:

- A **controller**: a single Raft group, run by the Gogs nodes themselves, that holds global metadata and decides, for each repository, which node is the **primary**.
- For each repository, a **primary** and **replicas**, with a per-repository **replication log**, a set of **in-sync replicas** (ISR), a **high watermark** (HW) and an **epoch** that protects against stale primaries.

Git and LFS objects never go through consensus: they travel via `git fetch` (or HTTP requests for LFS) and are verified by hash. Only their hashes go through consensus and the logs.

On each node, all logs and replication metadata live in a single [Pebble](https://github.com/cockroachdb/pebble) instance, while the relational state materialized from the logs lives in SQLite. The details are in the [local storage](#local-storage) section.

### Controller

The controller is a Raft group of 3 or 5 voting nodes, running inside regular Gogs nodes, like KRaft's "combined mode". Its replicated log contains:

- Cluster membership and node liveness.
- The **placement table**: for each repository, the assigned replicas, the primary, the ISR and the current epoch.
- Commands on the **global database**: users, organizations, teams, permissions, SSH keys and tokens.

Each node sends a heartbeat to the controller (e.g., every second). If a node is not heard from within the session timeout (e.g., 9 seconds, Kafka's default), the controller considers it dead and reassigns the primaries it hosted. The cost of failure detection is therefore proportional to the number of nodes, not to the number of repositories.

The controller's Raft log and its internal state (term, vote, configuration) live in Pebble. The state described by the log is materialized on each voting node in the global SQLite database. A Raft snapshot is a copy of that database plus the tail of the log after the copy.

### Primary, replicas and ISR

Each repository has a replication factor (default 3) and a minimum ISR (default 2). The controller places the replicas in different failure domains (configurable sites or zones) and appoints one of them as primary.

- The **primary** receives all writes for the repository and appends them to the replication log.
- The **replicas** follow the primary in pull mode: they ask for the entries after the last one they know, fetch the missing objects from the primary with `git fetch`, and acknowledge.
- The **ISR** is the set of caught-up replicas. A replica that falls behind beyond a time threshold leaves the ISR, but only through the controller.
- The **high watermark** is the last log entry present on all ISR members. Only entries up to the HW are committed.
- **Observer replicas** are asynchronous copies outside the ISR. They serve local reads in distant sites and disaster recovery, without slowing down writes.

A fork network (a repository and all its forks) always shares the same set of replicas, because pull requests between forks require both repositories on the same disk.

### Per-repository replication log

Each repository has an ordered log of entries. An entry looks like this:

```json
{
  "repo_id": 42,
  "seq": 1287,
  "epoch": 7,
  "kind": "ref_update",
  "updates": [
    { "ref": "refs/heads/main", "old": "4a1c…", "new": "9f2e…" }
  ]
}
```

The `kind` field distinguishes ref updates from commands on repository data (e.g., `issue_create`, `comment_create`). Commands already contain every value computed by the primary, including timestamps and identifiers, so that applying them on every replica is deterministic.

**The log is the source of truth. Refs on disk and rows in the local database are a materialization of it up to the HW.** A restarting node rebuilds its refs by replaying the log from the last applied point.

### Write path

A push goes through these steps:

1. The client connects to the nearest node, over HTTP ([`serviceRPC`](../../../internal/route/repo/http.go)) or SSH ([`runServ`](../../../cmd/gogs/serv.go)).
2. If that node is not the repository's primary, it forwards the request to the primary listed in the placement table.
3. On the primary, `git receive-pack` receives the pack and keeps it in quarantine (`GIT_QUARANTINE_PATH`, Git 2.13 and later) while it runs the hooks.
4. In the pre-receive hook ([`runHookPreReceive`](../../../cmd/gogs/hook.go)), after the existing protected branch checks, the primary:
   1. Checks the compare-and-swap of each ref against the state at the head of the log, not against the disk.
   2. Appends the entry to the log with the current epoch.
   3. Waits until every ISR member has acknowledged both the entry and the objects it points to.
   4. Exits successfully when the HW reaches the entry, or with an error if the timeout expires or the ISR drops below the minimum.
5. `receive-pack` updates the local refs and replies to the client.
6. Side effects (webhooks, activity feed, pull request tests, which today are started by [`runHookPostReceive`](../../../cmd/gogs/hook.go) and [`TriggerTask`](../../../internal/route/repo/tasks.go)) are run only by the primary, after the HW, using `(repo_id, seq)` as the idempotency key.

Two concurrent pushes to the same ref are serialized by the log: the second fails the compare-and-swap at step 4.1, and the client receives `rejected (non-fast-forward)`, as on a single server.

Web writes that concern a repository (issues, comments, settings) follow the same path, as commands in the repository's log. Global writes (users, permissions) go through the controller leader.

### Read path

Every replica and every observer serves clone, fetch and web pages with the state up to its own HW. Reads can therefore lag slightly behind the primary. When a read must see a write that was just made (e.g., the page shown right after creating an issue), the node forwards the read to the primary, or waits until it has applied the entry indicated by the primary.

### Failover and fencing

When the controller declares a primary dead, it:

1. Picks the new primary **only among ISR members**.
2. Increments the repository's epoch and records the new primary and the new ISR in its own log.
3. Sends the new epoch to the replicas.

A primary that is isolated but still running (the "zombie primary") may still accept a connection, but it cannot commit anything: the replicas know the new epoch and reject every entry with a lower epoch, so its HW never advances and the client never receives an acknowledgment. To limit how long clients wait, a primary that cannot reach the controller for a configurable interval stops accepting writes on its own.

When the old primary comes back, it asks the new primary for the last entry of its own last epoch, truncates its log at that point and resets its refs to the corresponding state. This is Kafka's approach (KIP-101 and KIP-279), preferred over simply truncating to the HW, which can lose entries or let them diverge. Any extra objects left on disk are harmless and are removed by the regular `git gc`.

A planned primary change (e.g., to move it closer to the users who write the most) uses the same mechanism, picking an ISR member that is already caught up.

### Safety rules

These invariants hold at all times. Every implementation and every fault test verifies them.

1. **Clean election.** The new primary is chosen only among ISR members. A replica that is not caught up is never elected, not even manually, without an explicit administrative operation that acknowledges possible data loss.
2. **ISR managed by the controller.** The primary may ask to remove or add a replica, but the change only takes effect once the controller records it. Otherwise a primary could commit writes on a shrunken ISR, die, and let a replica that lacks them be promoted.
3. **Minimum ISR.** With a replication factor of 3, the minimum ISR is 2. If the ISR drops below the minimum, writes are rejected instead of being committed on a single disk.
4. **Epoch on every message.** Every entry, replication request and acknowledgment carries the epoch. A recipient that sees an epoch lower than the one it knows rejects the message.
5. **Objects before refs.** An entry is committed only when every ISR member has both the entry and all objects reachable from the new ref values. No ref, on any node, ever points to a missing object.
6. **No exposure beyond the HW.** A node never shows clients refs or repository data beyond its own HW.
7. **Side effects exactly once.** Webhooks, cron jobs, mirror syncs and other outward effects are run by a single node: the primary for those of a repository, the controller leader for global ones. Every effect carries an idempotency key.
8. **Determinism.** Commands in the logs contain precomputed values. No replica generates timestamps, random numbers or identifiers while applying them.
9. **Atomic apply.** The changes produced by an entry and the "applied up to `seq`" index are written to SQLite in the same transaction. After a crash, a node resumes from the saved index, without applying the same entry twice and without skipping any.

### Database data

The current database is split into two parts, both pure Go SQLite (already supported through `glebarez/sqlite` in [`go.mod`](../../../go.mod)):

- **Global database**: users, organizations, teams, permissions, keys, tokens and the placement table. It is materialized from the controller's log.
- **Local repository database**: issues, pull requests, comments, releases, labels and milestones. Each node has one, with the data of the repositories it hosts, materialized from the individual repository logs.

Ephemeral traffic stays out of the logs: sessions become signed cookies, the cache stays local with short TTLs, and counters such as visits or last login are aggregated and written periodically.

Views that span many repositories (dashboard, search, "my issues", administration statistics) can no longer use `JOIN` on a single database. They become distributed queries across nodes, or global indexes updated asynchronously.

### Local storage

Each node uses two embedded storage engines, both in pure Go, each for the shape of data it is designed for.

**Pebble for logs and replication metadata.** A single Pebble instance per node contains:

- The controller's Raft log and its internal state.
- The replication logs of all repositories hosted by the node.
- For each repository, the current epoch, the HW and the last received entry.

Keys are prefixed by type and by repository, with numbers encoded in big-endian so that byte order matches numeric order:

```text
raft/log/<index>                 controller log entries
raft/state/<name>                term, vote, configuration
repo/<repo_id>/log/<seq>         replication log entries
repo/<repo_id>/meta/<name>       epoch, HW, last received entry
```

This layout makes the frequent operations simple:

- Reading the entries after a given `seq` is an iterator over a key range.
- Compacting a log, or truncating it after a failover, is a single range deletion.
- Moving a repository to another node means exporting its key range to SST files and ingesting them at the destination.
- Many small concurrent writes, from different repositories, are grouped into a single fsync of Pebble's WAL.

**SQLite for materialized state.** The global database and the local repository database stay in SQLite, with the current relational schema and queries of [`internal/database`](../../../internal/database). Gogs today has 38 tables and hundreds of queries with filters, `JOIN`, ordering and pagination. Rewriting them on a key-value model would bring no benefit to the architecture.

**Durability.** An entry is safe once it is in Pebble's WAL on every ISR member, or on a majority for the controller's log. SQLite is a materialization that can be rebuilt from the logs, so it can run with `synchronous=NORMAL` and save one fsync per write. This holds on one condition: the logs must be retained at least back to the last SQLite snapshot. A full snapshot of a node is a copy of the SQLite databases (with `VACUUM INTO` or the backup API) plus the tail of the logs after the copy.

Alternatives considered for local storage:

| Option | Outcome | Reason |
|---|---|---|
| SQLite only, logs included | rejected | millions of append-only logs become huge tables, with truncation and compaction done row by row |
| Pebble only, state included | rejected | requires rewriting the whole Gogs data layer as a key-value model, with secondary indexes, counts and migrations handled by hand |
| bbolt for logs | rejected | a single writer at a time, write amplification with large logs, and the file does not shrink after deletions |
| Pebble for logs, SQLite for state | **chosen** | each engine handles the shape of data it is designed for, as in rqlite (Raft log in a KV store, state in SQLite) |

### Gogs components to adapt

| Component | Today | In the cluster |
|---|---|---|
| HTTP and SSH push | run locally | forwarded to the primary, acknowledged in the pre-receive hook after the HW |
| Hooks (`gogs hook`) | write directly to the database | go through the primary via an internal API |
| Sessions | in memory | signed cookies |
| Cache | in memory | local, with short TTLs |
| Cron ([`internal/cron`](../../../internal/cron/cron.go)) | run by every process | run by the controller leader or by the repository primary |
| Mirrors ([`SyncMirrors`](../../../internal/database/mirror.go)) | synced locally | synced by the primary, with the result entering the log as a ref update |
| Webhooks ([`internal/database/webhook.go`](../../../internal/database/webhook.go)) | delivered by every process | delivered by the primary with an idempotency key |
| LFS ([`internal/lfsx/storage.go`](../../../internal/lfsx/storage.go)) | local files | immutable files, replicated to the ISR before acknowledging the upload |
| Attachments and avatars | local files | same as LFS |
| SSH host keys ([`internal/ssh/ssh.go`](../../../internal/ssh/ssh.go)) | generated per node | identical on all nodes, stored in the controller |
| `git gc` | local | stays local and independent on each node |
| Service startup ([`initServices`](../../../cmd/gogs/internal/web/web.go)) | local queues and workers | in addition: controller member, log follower, replication workers |

## Options considered

### Option A: shared storage and external database

Today's high availability setup for Gogs and Gitea: several processes behind a load balancer, repositories on NFS or Ceph, and an external PostgreSQL or MySQL database.

| Dimension | Assessment |
|---|---|
| Complexity for Gogs | low |
| External dependencies | many: distributed filesystem and database |
| Scalability | limited by the shared filesystem |
| Write consistency | delegated to the filesystem and the database |

**Pros:** almost no code changes. **Cons:** violates the single binary requirement. Git on NFS is fragile (locks, metadata latency), and geographic distribution of a shared filesystem is hard.

### Option B: object storage and PostgreSQL

GitLab's model: objects on S3 or a compatible store, metadata on PostgreSQL, possibly with a repository replication layer such as Gitaly Cluster.

| Dimension | Assessment |
|---|---|
| Complexity for Gogs | high |
| External dependencies | many: object storage, PostgreSQL |
| Scalability | high |
| Write consistency | delegated to PostgreSQL |

**Pros:** an architecture proven at large scale. **Cons:** violates the single binary requirement, and it is exactly the dependency this project wants to remove.

### Option C: content distribution via BitTorrent or IPFS

Git bundles or packs distributed as torrents between nodes or to clients, with HTTP web seeds.

| Dimension | Assessment |
|---|---|
| Complexity for Gogs | medium |
| External dependencies | none |
| Scalability | high for reads of large, popular content |
| Write consistency | **no solution** |

**Pros:** reduces bandwidth for fresh clones of large public repositories. **Cons:** solves only the immutable part. It offers neither consensus on refs nor a write path. Packs generated by the smart protocol differ for every client and therefore cannot be shared. Between a few known and trusted nodes, trackers, the DHT and tit-for-tat add no value over `git fetch`. It remains possible in the future as an optional, independent layer for distribution to clients, through immutable bundles and `bundle-uri`.

### Option D: asynchronous primary-secondary replication

A primary accepts writes and sends them to secondaries with push mirrors, without waiting for acknowledgments.

| Dimension | Assessment |
|---|---|
| Complexity for Gogs | low |
| External dependencies | none |
| Scalability | limited by the single primary |
| Write consistency | weak |

**Pros:** simple. **Cons:** writes not yet replicated are lost on failover, and without consensus the risk of split brain is real. Violates requirement 3.

### Option E: a single Raft group with full replication

All writes, both refs and database, go through a single Raft log. Every node holds a full copy of everything.

| Dimension | Assessment |
|---|---|
| Complexity for Gogs | medium |
| External dependencies | none |
| Scalability | limited by what fits on one node and by the single leader |
| Write consistency | strong, waits for the fastest majority |

**Pros:** the simplest model with strong consistency. **Cons:** every node must hold every repository, and every write in the cluster goes through one leader, including writes from teams far away from it.

### Option F: sharded Multi-Raft

A global Raft group and a fixed number of shards, each with its own Raft group, repositories and leader, like CockroachDB or TiKV.

| Dimension | Assessment |
|---|---|
| Complexity for Gogs | high |
| External dependencies | none |
| Scalability | high |
| Write consistency | strong, waits for the fastest majority |

**Pros:** scales both space and writes, and tolerates failures without a central coordinator. **Cons:** many Raft groups to manage (heartbeats, elections, snapshots per group). Leadership is per shard, so all repositories in a shard share the same leader, and moving leadership closer to a repository's users is coarse-grained.

### Option G: Raft controller and per-repository primaries (chosen)

Described in the [decision](#decision) section.

| Dimension | Assessment |
|---|---|
| Complexity for Gogs | high |
| External dependencies | none |
| Scalability | high: one log and one metadata row per repository |
| Write consistency | strong, waits for the whole ISR |

**Pros:** a single consensus group, per-node failure detection, and failover and primary changes per individual repository. Pull replication matches `git fetch`. The model is proven in production by Kafka on millions of partitions, and its edge cases are documented. **Cons:** every write waits for the slowest in-sync replica, and failover depends on the controller being available.

### Option H: per-user namespaces and replication without consensus

Radicle's model: each participant has their own signed refs, replicated by gossip, with no official `main`.

**Pros:** no consensus needed, and it keeps working during long partitions. **Cons:** there is no authoritative version of the repository. It is a different product from a forge with permissions, protected branches and pull requests.

## Trade-off analysis

**Waiting for the ISR versus waiting for a majority.** Raft commits a write when the fastest majority replies. The ISR commits it when every in-sync replica has replied, so the latency is that of the slowest replica. With replicas in distant sites, this weighs on every push. There are three countermeasures, which can be combined: the controller places each repository's ISR in sites close to each other and to the primary, distant replicas become asynchronous observers, and a slow replica leaves the ISR (through the controller), temporarily accepting durability on 2 copies.

**Dependency on the controller.** If the controller loses its majority, existing primaries keep serving reads and writes as long as the ISR does not need to change, but no failover is possible. The controller must therefore run on 3 or 5 nodes in different sites, and its unavailability must be monitored as an incident.

**Durability and availability.** With a replication factor of 3 and a minimum ISR of 2, the system accepts writes with one node lost and rejects them with two nodes lost. This is the same behavior as a 3-node Raft group, chosen deliberately: rejecting a push is preferable to acknowledging it on a single disk.

**Disk space.** Each repository takes space equal to the replication factor, plus observers. This is the price of having no shared storage.

**Familiarity.** Raft and Kafka's replication protocol are well documented, but no existing forge uses this combination in a single binary. GitHub (Spokes) uses its own commit protocol over 3 replicas, GitLab (Gitaly Cluster) relies on PostgreSQL, and Gerrit multi-site relies on Kafka and Zookeeper. The project cannot copy an existing solution and must be validated with fault tests.

## Consequences

Easier:

- Installing a cluster: a single binary and one configuration file per node.
- Surviving the loss of a node or a site without losing acknowledged pushes.
- Serving reads from the nearest node.
- Moving a single repository, or its primary, closer to its users.
- Adding capacity by adding nodes.

Harder:

- Every database write goes through a log, so the code in [`internal/database`](../../../internal/database) must be reorganized into deterministic commands.
- Global views require distributed queries or asynchronous indexes.
- Schema migrations must be coordinated across nodes running different binary versions during a rolling upgrade.
- Debugging replication problems requires new tools (log state, ISR, epochs, replica lag).
- Each node manages two storage engines. Pebble's on-disk format only upgrades forward, so a binary upgrade must be planned like a migration, and log keys can only be inspected with dedicated tools.

To revisit later:

- The Raft library, after the phase 1 prototype.
- If the geographic latency of the ISR becomes a problem, majority acknowledgment for repositories with distant replicas.
- Distribution of clones to clients through bundles (option C), as an independent layer.

## Phases

Each phase ends with a verifiable criterion.

1. **Single-node preparation.** Introduce the placement lookup function (initially always the local node), split the global database from the repository database, turn writes into deterministic commands, and move sessions to signed cookies.
   - *Criterion:* all existing tests pass, and a single-node installation behaves as it does today.
2. **Controller.** A 3-node Raft group with its log in Pebble and the global database in SQLite, global writes forwarded to the leader, and global cron jobs run only on the leader.
   - *Criterion:* with 3 local nodes, killing the leader during continuous writes loses no acknowledged write.
3. **Per-repository log.** Primary, pull replicas, ISR, HW and epoch, with the logs in the node's Pebble instance. Pushes are acknowledged in the pre-receive hook, and replicas follow via `git fetch`.
   - *Criterion:* after every acknowledged push, `git rev-parse --all` and `git fsck` give the same result on all ISR members.
4. **Failover and operations.** Fencing, epoch-based truncation, planned primary changes, nodes joining and leaving, and cluster status tools.
   - *Criterion:* isolating the primary during continuous pushes never produces two different versions of an acknowledged ref.
5. **Repository data.** Issues, PRs, comments and releases as commands in the repository log, replication of LFS and attachments, and webhooks with an idempotency key.
   - *Criterion:* the same checks as phase 3 hold for the rows of the local repository database.
6. **Fault tests and geography.** Jepsen-style tests (nodes killed mid-push, simulated partitions, unreachable controller), observer replicas, and per-site placement rules.
   - *Criterion:* no violation of the [safety rules](#safety-rules) over an agreed number of randomized runs.

## Open questions

- Raft library for the controller: `hashicorp/raft` (high-level API, used by Consul and Nomad, needs a Pebble adapter for `LogStore` and `StableStore`) or `etcd-io/raft` (more flexible, with storage to implement anyway, and more code to write).
- Compaction policy for per-repository logs: after how many entries, or how much time, to keep only the tail after the last SQLite snapshot.
- Default values for the session timeout, the maximum ISR lag and the HW wait timeout during a push.
- Migration of an existing single-node installation to the cluster.
- Placement policy: how to express sites and zones in the configuration, and how to rebalance when a node is added.

## References

- Diego Ongaro and John Ousterhout, *In search of an understandable consensus algorithm* (2014), <https://raft.github.io/>.
- Kafka KIP-101, *Alter replication protocol to use leader epoch rather than high watermark for truncation*.
- Kafka KIP-279, *Fix log divergence between leader and follower after fast leader fail over*.
- Kafka KIP-500, *Replace ZooKeeper with a self-managed metadata quorum*.
- Kafka KIP-595, *A Raft protocol for the metadata quorum*.
- Kafka KIP-631, *The quorum-based Kafka controller*.
- Sanjay Ghemawat, Howard Gobioff and Shun-Tak Leung, *The Google file system* (2003), for the master and leased primary model.
- Git documentation on hooks, in particular pre-receive, reference-transaction and the quarantine environment: <https://git-scm.com/docs/githooks>.
- `hashicorp/raft`: <https://github.com/hashicorp/raft>.
- `etcd-io/raft`: <https://github.com/etcd-io/raft>.
- rqlite, as a reference for replicating SQLite via Raft: <https://github.com/rqlite/rqlite>.
- Pebble, CockroachDB's key-value storage engine: <https://github.com/cockroachdb/pebble>.
