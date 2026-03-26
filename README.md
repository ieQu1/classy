classy
=====

An application that helps managing a cluster of Erlang nodes.

# Concepts

- Site ID: a random unique identifier of the node that persists between the restarts and host name changes.
- Cluster ID: a random unique identifier of the cluster.
- Run level: global system state derived from the configuration and the number of peers.
  There are the following run levels:
  + `stopped`: classy is stopped
  + `single`: classy application is running and exchanging membership information
  + `cluster`: number of known peers is >= `n_sites` configuration parameter.
  + `quorum`: number of running peers is >= `quorum` configuration parameter.

# Configuration

`classy` is configured via OTP application environment variables and callbacks.

## `classy.setup_hooks`

Type: `mfa()`.

A callback that classy executes during startup from `classy_hook:init/0`.
It allows the business application to register hooks using the `classy:on_...`
API instead of writing raw values directly into the application environment.

Notes:

- The callback runs during application startup, before the local site and
  cluster are initialized.
- If the callback crashes, `classy` startup fails.

## `classy.table_dir`

Type: `file:filename()`.

Default: `"."`

Directory where `classy_table` stores its local persistent files.

Current behavior:

- `classy` does not create the directory automatically. It must already exist.
- The directory must be writable, because the table implementation opens a
  `disk_log` file there and appends updates to it.
- If the directory does not exist or is not writable, opening the table fails,
  which causes the corresponding `classy_table` server to crash. In practice
  this means `classy` startup fails, or a membership worker fails to start.

Operationally, this directory contains only local node state. It is not shared
with other nodes.

## `classy.sync_timeout`

Type: `non_neg_integer()`.

Unit: ms.

Maximum batching delay before local membership updates are synced to peers.

Current behavior:

- When membership state changes, `classy_membership` schedules a sync to peers
  after `sync_timeout` milliseconds.
- Additional updates that happen before that timer fires are batched into the
  same sync.
- This is not a network timeout and not a retry deadline. When the timer fires,
  the node sends its current incremental state to peers.
- If a sync was already scheduled sooner, later updates do not postpone it.

Trade-off:

- Lower values propagate membership changes faster, but create more sync traffic.
- Higher values reduce traffic by batching more updates, but increase the time
  it takes other nodes to observe changes.

## `classy.rpc_timeout`

Type: `timeout()`.

Unit: ms.

Default: 5s.

Default timeout for remote procedure calls made through `classy_lib:rpc_timeout/0`.

Current behavior in this repository:

- It is used by `classy_node:join_node/2` for the initial `rpc:call/5` to the
  target node's `classy_node:hello/0`.
- If that RPC times out or otherwise fails, the join fails and `join_node/2`
  returns `{error, Reason}` where `Reason` is the `rpc:call/5` result.
- Membership state propagation after a successful join does not use this
  timeout; it uses asynchronous casts.

## `classy.forget_after`

Type: `pos_integer()`

Unit: s.

Default: 1w.

Retention period for metadata about kicked sites when
`classy_membership:cleanup/3` is invoked.

Important: in the current codebase, this value is documented but not consumed
automatically from the application environment. There is no enabled background
cleanup worker in this repository. Cleanup only happens if some external code
calls `classy_membership:cleanup/3` and passes a value explicitly.

When cleanup is run, sites are forgotten only if all of the following are true:

1. The site is marked as not a member.
2. It has been in that state for at least `forget_after` seconds.
3. Active peers have already acknowledged the relevant membership updates.

Cleanup may lead to the following situation:

1. Site A goes down
2. Site B kicks A
3. Information about event 2 propagates throughout the cluster
4. Cleanup. All active peers delete data about A.
5. A goes back up

Since step 4 removes the record that A was kicked, A may reappear in the
cluster when it comes back online.

So `forget_after` should be set to a fairly large value if nodes can return
after long outages.

## `classy.n_sites`

Type: `pos_integer()`,

Default: 1.

Minimum number of known cluster members required to advance the local run level
from `single` to `cluster`.

Current behavior:

- This compares against `classy:sites/0`, not against currently running nodes.
- A site may therefore reach `cluster` even if some known members are down.

## `classy.quorum`

Type: `pos_integer()`,

Default: 1.

Minimum number of running nodes required to advance the local run level from
`cluster` to `quorum`.

Current behavior:

- The local run level becomes `quorum` only if both conditions hold:
  the number of known sites is at least `n_sites`, and the number of running
  nodes is at least `quorum`.
- The helper `classy:quorum(config)` clamps the configured value to at least 1.

# Setting default site and cluster

By default, classy initializes site to a random value,
and the same value is used for the cluster ID.

Business applications can override this behavior by registering an
`on_node_init` hook containing a call to `classy_node:maybe_init_the_site/1`:

```
classy:on_node_init(
  fun() ->
      classy_node:maybe_init_the_site(SiteId)
  end,
  0)
```
