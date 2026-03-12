classy
=====

An application that helps managing a cluster of Erlang nodes

# Configuration

`classy` is configured via OTP application environment variables and callbacks.

## `classy.setup_hooks`

Type: `mfa()`.
A callback that is classy executes during startup.
It allows business application to set up other hooks using a more type-safe API.


## `classy.table_dir`

Type: `file:filename()`.
Default: `"."`
Directory where persistent data is stored.
It should be writable.

## `classy.sync_timeout`

Type: `non_neg_integer()`.
Unit: ms.
Maximum interval of time that can pass between the cluster CRDT server receiving an update and the moment it propagates it to the peers.

## `classy.rpc_timeout`

Type: `timeout()`.
Unit: ms.
Default: 5s.
Default timeout for remote procedure calls.

## `classy.forget_after`

Type: `pos_integer()`
Unit: s.
Default: 1w.
Forget information about inactive (kicked) sites after this period of time.

Note: cleanup procedure may lead to the following situation:

1. Site A goes down
2. Site B kicks A
3. Information about event 2 propagates through out the cluster.
4. Cleanup. All active peers delete data about A.
5. A goes back up

Since at step 4 we removed the data about event 2., A will reappear in the cluster.

So `forget_timeout` should be set to a fairly large value to make sure it doesn't cover nodes that can go back online.

# Setting default site and cluster

By default, classy initializes site to a random value,
and the same value is used for the cluster ID.

Business applications can override this behavior by registering `on_node_init` hook containing a call to `classy_node:maybe_init_the_cluster`:

```
classy:on_node_init(
  fun() ->
      classy_node:maybe_init_the_site(ClusterId, SiteId)
  end,
  0)
```
