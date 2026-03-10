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
Directory where persistent data is stored.
It should be writable.

## `classy.sync_timeout`

Type: `non_neg_integer()`.
Maximum interval of time that can pass between the cluster CRDT server receiving an update and the moment it propagates it to the peers.

## `classy.rpc_timeout`

Type: `timeout()`.
Default timeout for remote procedure calls.

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
