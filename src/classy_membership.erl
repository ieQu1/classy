%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_membership).
-moduledoc """
# Cluster Membership CRDT

This module provides low-level API for maintaining and updating cluster membership information.
Business code should not use it directly.

# Logs

Each site `S` maintains a command log `l[S]`.
When a command (such as `join(S)` or `leave(S)`) is executed on a site,
it is added to the log.

Sites constantly try to broadcast their logs to known peers.

# Logical Clocks

Each site is associated with a Lamport clock.
Value of the clock of site `A`, as viewed on site `B`, is denoted as `c[A, B]`.

Clocks are updated in the following cases:

- `c[A, A]` is incremented when site `A` appends a new command to `l[A]`.
  Entries in `l[A]` are indexed by `c[A,A]`.
- `c[A, B]` is incremented when `B` appends a command concerning `A` to `l[B]`.
  New value of the clock is added to the command body.
- `c[A, B]` is synced when `B` receives a message from or about `A`.

# Event ordering

Eventual consistency is assured by the existence of a total order of log entries.
Provided that each site received all log entries from all peers (in any order),
it derives peer membership states from the log entries of the maximum order.

More formally,
let `s[A, B]` be state of site `A` as perceived by `B`,
and `L` be a concatenation of all logs from all sites,
then `s[A, B] = to_state(maximum(fun(X, Y) -> ord(X) > ord(Y) end, filter(concerning_site(B), L)))`.

If comparison function is transitive,
and `ord(X) = ord(Y) → to_state(X) = to_state(Y)`,
then the above expression returns the same value for every permutation of `L`.

This module uses lexicographic order of `{c[Target, Origin], Magic, Origin}` triples as the event order.
It satisfies the above conditions because
a) lexicographic order is known to behave well.
b) `{c[Target, Origin], Origin}` pair uniquely identifies `Target` membership state,
   because Origin always increments its clock after issuing a command about Target.

Note that this total order is *not* causal.
Practically, lack of strict causality means that cluster will eventually converge to the same state,
but earlier join/leave commands may override later commands.

These adverse side effects can be observed when conflicting commands are issued on different nodes faster than the nodes sync with each other.
This is most likely to happen during a network partition.

# Log Syncing

Typical scenario:

```
  A                      B
  |                      |
  |--[{1,a}]------------>|
  |                      |
  |<--ack 1--------------|
  |                      |
  |--[{2,b}]------------>|
  |                      |
  |<--ack 2--------------|
  ...
```

Possible scenarios:

```
  A                      B
  |                      |
  |--[{1,a}]------------>|
  |                      |
  |--[{1,a},{2,b}]------>|
  |                      |
  |<--ack 1--------------|
  |                      |
  |--[{2,b},{3,c}]------>|
  |                      |
  |-[{2,b},{3,c},{4,d}]->|
  |                      |
  |<--ack 3--------------|
  ...
```

or

```
  A                      B
  |                      |
  |                      |
  |--[{2,b},{3,c}]------>|
  |                      |
  |<--ack 0--------------|
  |                      |
  |-[{1,a},{2,b},{3,c}]->|
  |                      |
  ...
```

""".


-behavior(gen_server).

%% API:
-export([join/4, kick/4, members/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([create_tables/0, delete_tables/0, start_link/1]).

-export_type([start_args/0, op/0, ord/0, clock/0, mementry/0]).

-include("classy_rt.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(default_sync_timeout, 100).

-type start_args() ::
        #{ module  := module()
         , cluster := classy:cluster_id()
         , site    := classy:site()
         }.

-type clock() :: non_neg_integer().

-doc """
Arbitrary term used to break ties between commands with the same logical timestamp.
""".
-type magic() :: term().

-doc """
The following command is used to set membership state of `target` site.
""".
-record(op_set,
        { %% Site that issued the command:
          origin :: classy:site()
          %% Site which is being updated:
        , target :: classy:site()
          %% c[target, origin]:
        , c :: clock()
          %% This term can be used to break ties:
        , m :: magic()
          %% Is member?
        , mem :: boolean()
          %% Origin's wall time when the update was made. This value
          %% isn't used by this module directly, but it gives hint to
          %% the autoclean:
        , owt :: integer()
        }).

-type op() :: #op_set{}.

-doc """
Projection of `op()` fields used to establish total order of logs.
""".
-type ord() :: {clock(), magic(), classy:site()}.

%% `site' sends a portion of its logs that is newer than `since':
-record(cast_sync,
        { from :: classy:site()
        , since :: clock()
        , acked :: clock()
          %% c[from, from]
        , c :: clock()
        , data :: [op()]
        }).

-doc "Timeout message triggering syncing out state.".
-record(to_sync_out, {}).

-record(call_join, {target :: classy:site()}).
-record(call_kick, {target :: classy:site()}).

%% Type of data stored in ets:
-record(memtable, {k, op, mem, tou}).
-define(memtable, ?MODULE).
-type mementry() ::
        #memtable{ k :: {classy:cluster_id(), _Local :: classy:site(), _Target :: classy:site()}
                 , op :: op()
                   %% State, immeditely derived from `op':
                 , mem :: boolean()
                   %% Local logical time of the last update to the entry:
                 , tou :: clock()
                 }.

-record(s,
        { %% Cluster ID:
          cluster :: classy:cluster_id()
          %% Local site id:
        , site :: classy:site()
          %% Runtime callback module:
        , cbm :: module()
          %% State of the runtime CBM:
        , cbs :: classy_rt:cbs()
        , sync_timer :: undefined | reference()
          %% Clock of the last successful sync *from* site:
        , acked_in :: #{classy:site() => clock()}
          %% Clock of the last successful sync *to* site:
        , acked_out :: #{classy:site() => clock()}
          %% Logical clock:
        , clock :: clock()
        }).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Low-level call that sets `Target`'s membership state to `true`.

WARNING: this function does not check if target site exists and/or is part of cluster.
When called with invalid `Target`,
it will create a new entry that will eventually make its way to the entire cluster.
This fictitious site will exist in `down` state until kicked,
and even then some records about it may be kept around.
""".
-spec join(module(), classy:cluster_id(), classy:site(), classy:site()) -> ok | {error, _}.
join(CBM, Cluster, Local, Target) ->
  case classy_rt:get_membership_pid(CBM, Cluster, Local) of
    Pid when is_pid(Pid) ->
      try
        gen_server:call(Pid, #call_join{target = Target})
      catch
        EC:Err -> {error, {EC, Err}}
      end;
    undefined ->
      {error, noproc}
  end.

-doc """
Low-level call that sets `Target`'s membership state to `false`.

Note: kicking the site doesn't erase information about it.
Nodes will continue to propagate a record saying that target site is not part of the cluster.
""".
-spec kick(module(), classy:cluster_id(), classy:site(), classy:site()) -> ok.
kick(CBM, Cluster, Local, Target) ->
  case classy_rt:get_membership_pid(CBM, Cluster, Local) of
    Pid when is_pid(Pid) ->
      try
        gen_server:call(Pid, #call_kick{target = Target})
      catch
        EC:Err -> {error, {EC, Err}}
      end;
    undefined ->
      {error, noproc}
  end.

-doc """
Return active members of the `Cluster`,
as perceived by `Local` site.

WARNING: if `Local` is not a member of the returned list,
then the local system may be permanently out of sync with the `Cluster` or `{Cluster,Local}` server may be inactive.
In both cases the result value can't be trusted.
""".
-spec members(classy:cluster_id(), classy:site()) -> [classy:site()].
members(Cluster, Local) ->
  MS = {#memtable{k = {Cluster, Local, '$1'}, mem = true, _ = '_'}, [], ['$1']},
  ets:select(?memtable, [MS]).

%%================================================================================
%% Internal exports
%%================================================================================

-doc """
This function should be called by the supervisor before starting any instances of this server.
""".
-spec create_tables() -> ok.
create_tables() ->
  ets:new(?memtable, [named_table, public, {keypos, #memtable.k}]),
  ok.

-spec delete_tables() -> ok.
delete_tables() ->
  ets:delete(?memtable),
  ok.

-spec start_link(start_args()) -> {ok, pid()}.
start_link(Args = #{module := CBM, cluster := _, site := _}) when is_atom(CBM) ->
  gen_server:start_link(?MODULE, Args, []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec init(start_args()) -> {ok, #s{}}.
init(#{module := CBM, cluster := Cluster, site := Site}) ->
  process_flag(trap_exit, true),
  {ok, CBS} = classy_rt:init(CBM, Cluster, Site),
  S = #s{ cluster = Cluster
        , site = Site
        , cbm = CBM
        , cbs = CBS
        , acked_in = restore_acked_in(CBM, CBS)
        , acked_out = restore_acked_out(CBM, CBS)
        , clock = restore_clock(CBM, CBS)
        },
  memtab_restore(S),
  {ok, need_sync(0, S)}.

handle_call(#call_join{target = Target}, _From, S0) ->
  S = local_command(join, Target, S0),
  {reply, ok, S};
handle_call(#call_kick{target = Target}, _From, S0) ->
  S = local_command(kick, Target, S0),
  {reply, ok, S};
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

handle_cast(#cast_sync{} = Req, S) ->
  {noreply, handle_sync_in(Req, S)};
handle_cast(_Cast, S) ->
  {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(#to_sync_out{}, S0 = #s{cbm = CBM, cbs = CBS}) ->
  S = S0#s{sync_timer = undefined},
  ok = classy_rt:pflush(CBM, CBS),
  {noreply, handle_sync_out(S)};
handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, S = #s{cbm = CBM, cbs = CBS}) ->
  memtab_clean(S),
  ok = classy_rt:pflush(CBM, CBS),
  classy_rt:terminate(CBM, CBS).

%%================================================================================
%% Internal functions
%%================================================================================

-spec ord(op()) -> ord().
ord(#op_set{c = C, m = M, origin = O}) ->
  {C, M, O}.

-spec state(op()) -> boolean().
state(#op_set{mem = Mem}) ->
  Mem.

-spec local_command(join | kick, classy:site(), #s{}) -> #s{}.
local_command(Cmd, Target, S0 = #s{cbm = CBM, site = Local}) ->
  {C, S} = inc_get_clock(S0),
  Op = #op_set{ origin = Local
              , target = Target
              , c = C
              , mem = case Cmd of
                        join -> true;
                        kick -> false
                      end
              , owt = classy_rt:time_s(CBM)
              },
  _ = merge(C, Op, S),
  need_sync(S).

-spec handle_sync_in(#cast_sync{}, #s{}) -> #s{}.
handle_sync_in(Req, S0) ->
  #cast_sync{ from = From
            , since = Since
            , c = Cf
            , acked = AckedOut
            , data = Data
            } = Req,
  case get_acked_in(From, S0) >= Since of
    true ->
      {Cl, S} = inc_get_clock(sync_clock(Cf, S0)),
      lists:foreach(
        fun(Op) -> merge(Cl, Op, S) end,
        Data),
      need_sync(
        set_acked_in(
          From, Cf,
          set_acked_out(From, AckedOut, S)));
     false ->
      %% Gap in sequence. Ignore this message. Peer will be notified
      %% about the expected acked value during the next sync-out:
      need_sync(S0)
  end.

-spec handle_sync_out(#s{}) -> #s{}.
handle_sync_out(S = #s{cbm = CBM, cluster = Cluster, site = Local, clock = C}) ->
  lists:foreach(
    fun(Site) ->
        case classy_rt:get_membership_pid(CBM, Cluster, Site) of
          Pid when is_pid(Pid) ->
            Since = get_acked_out(Site, S),
            Data = memtab_since(Since, S),
            gen_server:cast(
              Pid,
              #cast_sync{ from = Local
                        , since = Since
                        , acked = get_acked_in(Site, S)
                        , c = C
                        , data = Data
                        });
          undefined ->
            ok
        end
    end,
    peers(S)),
  S.

-spec merge(clock(), op(), #s{}) -> boolean().
merge(LTime, Op, S) ->
  #op_set{target = Site} = Op,
  case memtab_lookup(Site, S) of
    {ok, Op0} ->
      case ord(Op) > ord(Op0) of
        true ->
          set_last(LTime, Op, S),
          true;
        false ->
          false
      end;
    undefined ->
      set_last(LTime, Op, S),
      true
  end.

%%--------------------------------------------------------------------------------
%% Interface for site state storage
%%--------------------------------------------------------------------------------

-doc """
Apply operation to the persistent state and memtable.
""".
-spec set_last(clock(), op(), #s{}) -> #s{}.
set_last(LTime, Op, S = #s{cbm = CBM, cbs = CBS}) ->
  #op_set{target = Target} = Op,
  ok = classy_rt:pset(CBM, CBS, {?cl_last, Target}, Op),
  memtab_set(LTime, Op, S).

-spec memtab_lookup(classy:site(), #s{}) -> {ok, op()} | undefined.
memtab_lookup(Site, #s{cluster = Cluster, site = Local}) ->
  case ets:lookup(?memtable, {Cluster, Local, Site}) of
    [#memtable{op = Op}] ->
      {ok, Op};
    [] ->
      undefined
  end.

-spec memtab_since(clock(), #s{}) -> [op()].
memtab_since(Since, #s{cluster = Cluster, site = Local}) ->
  MS = { #memtable{ k = {Cluster, Local, '_'}
                  , op = '$1'
                  , tou = '$2'
                  , _ = '_'
                  }
       , [{'>=', '$2', Since}]
       , ['$1']
       },
  ets:select(?memtable, [MS]).

-spec memtab_restore(#s{}) -> ok.
memtab_restore(S = #s{cbm = CBM, cbs = CBS, clock = Cl}) ->
  memtab_clean(S),
  L = classy_rt:plist(CBM, CBS, ?cl_last),
  lists:foreach(
    fun({_Target, Op}) ->
        memtab_set(Cl, Op, S)
    end,
    L).

-spec memtab_set(clock(), op(), #s{}) -> #s{}.
memtab_set(LTime, Op, S = #s{cluster = Cluster, site = Local}) ->
  #op_set{target = Target} = Op,
  MemEntry = #memtable{ k = {Cluster, Local, Target}
                      , op = Op
                      , tou = LTime
                      , mem = state(Op)
                      },
  ets:insert(?memtable, MemEntry),
  S.

-spec memtab_clean(#s{}) -> ok.
memtab_clean(#s{cluster = Cluster, site = Local}) ->
  ets:match_delete(?memtable, #memtable{k = {Cluster, Local, '_'}, _ = '_'}),
  ok.

-spec peers(#s{}) -> [classy:site()].
peers(#s{cluster = Cluster, site = Local}) ->
  MS = {#memtable{k = {Cluster, Local, '$1'}, _ = '_'}, [], ['$1']},
  ets:select(?memtable, [MS]).

%%--------------------------------------------------------------------------------
%% Logical clocks
%%--------------------------------------------------------------------------------

-spec inc_get_clock(#s{}) -> {clock(), #s{}}.
inc_get_clock(S0 = #s{clock = C0}) ->
  C = C0 + 1,
  S = S0#s{clock = C},
  psave_clock(S),
  {C, S}.

-spec sync_clock(clock(), #s{}) -> #s{}.
sync_clock(T1, S0 = #s{clock = T0}) ->
  S = S0#s{clock = max(T0, T1)},
  psave_clock(S),
  S.

-spec restore_clock(module(), classy_rt:cbs()) -> clock().
restore_clock(CBM, CBS) ->
  case classy_rt:plist(CBM, CBS, ?cl_clock) of
    [C] when is_integer(C) -> C;
    [] -> 0
  end.

-spec psave_clock(#s{}) -> ok.
psave_clock(#s{cbm = CBM, cbs = CBS, clock = C}) ->
  classy_rt:pset(CBM, CBS, ?cl_clock, C).

%%--------------------------------------------------------------------------------
%% Functions related to the p2p protocol
%%--------------------------------------------------------------------------------

-doc """
Schedule sync with the peers after the default timeout.
""".
-spec need_sync(#s{}) -> #s{}.
need_sync(S) ->
  need_sync(
    application:get_env(classy, sync_timeout, ?default_sync_timeout),
    S).

-doc """
Schedule sync with the peers after a specified timeout.
This call has no effect if the sync is already scheduled.
""".
-spec need_sync(non_neg_integer(), #s{}) -> #s{}.
need_sync(_Timeout, S = #s{sync_timer = R}) when is_reference(R) ->
  S;
need_sync(Timeout, S = #s{sync_timer = undefined}) ->
  TRef = erlang:send_after(Timeout, self(), #to_sync_out{}),
  S#s{sync_timer = TRef}.

-spec get_acked_in(classy:site(), #s{}) -> clock().
get_acked_in(Site, #s{acked_in = A}) ->
  maps:get(Site, A, 0).

-spec get_acked_out(classy:site(), #s{}) -> clock().
get_acked_out(Site, #s{acked_out = A}) ->
  maps:get(Site, A, 0).

-spec set_acked_in(classy:site(), clock(), #s{}) -> #s{}.
set_acked_in(Site, Clock, S = #s{cbm = CBM, cbs = CBS, acked_in = A}) ->
  ok = classy_rt:pset(CBM, CBS, {?cl_acked_in, Site}, Clock),
  S#s{acked_in = A#{Site => Clock}}.

-spec set_acked_out(classy:site(), clock(), #s{}) -> #s{}.
set_acked_out(Site, Clock, S = #s{cbm = CBM, cbs = CBS, acked_out = A}) ->
  ok = classy_rt:pset(CBM, CBS, {?cl_acked_in, Site}, Clock),
  S#s{acked_out = A#{Site => Clock}}.

%%--------------------------------------------------------------------------------
%% Persistent state save/restore
%%--------------------------------------------------------------------------------

-spec restore_acked_in(module(), classy_rt:cbs()) -> #{classy:site() => clock()}.
restore_acked_in(CBM, CBS) ->
  maps:from_list(classy_rt:plist(CBM, CBS, ?cl_acked_in)).

-spec restore_acked_out(module(), classy_rt:cbs()) -> #{classy:site() => clock()}.
restore_acked_out(CBM, CBS) ->
  maps:from_list(classy_rt:plist(CBM, CBS, ?cl_acked_out)).
