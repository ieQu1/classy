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
-export([start_link/1, handshake/2]).

-export_type([start_args/0, op/0, ord/0, clock/0, lentry/0]).

-include("classy_rt.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(reconnect_time, 1_000).
-define(ttl, 5).

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
The following command is used to update cluster membership of `target` site.
""".
-record(l_set,
        { %% Site that issued the command.
          %% This field is necessary for keeping deterministic order
          %% of commands that peers copy from others' logs
          origin :: classy:site()
        , target :: classy:site()
          %% c[target, origin]:
        , c :: clock()
        , m :: magic()
          %% Is member?
        , mem :: boolean()
        }).

-type op() :: #l_set{}.

-doc """
Projection of `op()` fields used to establish total order of logs.
""".
-type ord() :: {clock(), magic(), classy:site()}.

-record(lentry,
        { id :: clock()
        , ttl :: pos_integer()
        , payload :: op()
        }).

-doc """
Envelope for the log entries.
""".
-type lentry() :: #lentry{}.

-record(call_handshake, {from :: classy:site()}).
%% Server periodically sends this message to itself to refresh
%% connections that went down:
-record(cast_reconnect, {}).

%% `site' sends a portion of its logs that is newer than `since':
-record(cast_push,
        { from :: classy:site()
        , since :: clock()
        , log :: [lentry()]
        }).
%% Set acked log entry for `site' to `acked':
-record(cast_ack,
        { site :: classy:site()
        , acked :: clock()
        }).

-record(call_join, {target :: classy:site()}).
-record(call_kick, {target :: classy:site()}).

-record(conn,
        { mref :: reference()
        , pid :: pid()
        , acked :: clock()
        }).

-record(s,
        { %% Cluster cookie:
          cluster :: classy:cluster_id()
          %% My site id:
        , site :: classy:site()
          %% Callback module:
        , cbm :: module()
          %% State of the callback module:
        , cbs :: classy_rt:cbs()
          %% Log of outgoing commands. Log is stored in reverse
          %% chronological order, with newer entries closer to the
          %% head.
        , log :: [lentry()]
          %% Acked index in the remote log:
        , acked :: #{classy:site() => clock()}
          %% Clocks:
        , clocks :: #{classy:site() => clock()}
          %% Last entry for each site, used to derive membership state:
        , last :: #{classy:site() => op()}
          %% Live p2p connections:
        , conns = #{} :: #{classy:site() => #conn{}}
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
Nodes will keep and propagate a record saying that target site is not part of the cluster.
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

-spec members(classy:cluster_id(), classy:site()) -> [classy:site()].
members(Cluster, Local) ->
  error(todo).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link(start_args()) -> {ok, pid()}.
start_link(Args = #{module := CBM, cluster := _, site := _}) when is_atom(CBM) ->
  gen_server:start_link(?MODULE, Args, []).

-spec handshake(pid(), classy:site()) -> {ok, classy:cluster_id(), classy:site(), clock()} | {error, _}.
handshake(Pid, Local) ->
  try
    gen_server:call(Pid, #call_handshake{from = Local})
  catch
    EC:Err ->
      {error, {EC, Err}}
  end.

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
        , log = restore_log(CBM, CBS)
        , acked = restore_acked(CBM, CBS)
        , last = restore_last(CBM, CBS)
        , clocks = restore_clocks(CBM, CBS)
        },
  {ok, reconnect(S)}.

handle_call(#call_handshake{from = From}, _From, S) ->
  #s{ cluster = Cluster
    , site = Local
    } = S,
  Reply = {ok, Cluster, Local, get_acked(From, S)},
  {reply, Reply, S};
handle_call(#call_join{target = Target}, _From, S0) ->
  S = create_and_append(join, Target, S0),
  {reply, ok, S};
handle_call(#call_kick{target = Target}, _From, S0) ->
  S = create_and_append(kick, Target, S0),
  {reply, ok, S};
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

handle_cast(#cast_reconnect{}, S) ->
  {noreply, reconnect(S)};
handle_cast(#cast_ack{site = Site, acked = Acked}, S) ->
  {noreply, handle_ack(Site, Acked, S)};
handle_cast(#cast_push{from = From, since = Since, log = Log}, S) ->
  {noreply, handle_push(From, Since, Log, S)};
handle_cast(_Cast, S) ->
  {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info({'DOWN', MRef, _Type, _Obj, _Reason}, S0 = #s{conns = Conns0}) ->
  case site_of_conn(MRef, S0) of
    {ok, Site} ->
      Conns = maps:remove(Site, Conns0),
      S = S0#s{conns = Conns},
      {noreply, do_reconnect(Site, S)};
    undefined ->
      {noreply, S0}
  end;
handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, #s{cbm = CBM, cbs = CBS}) ->
  classy_rt:terminate(CBM, CBS).

%%================================================================================
%% Internal functions
%%================================================================================

-spec ord(op()) -> ord().
ord(#l_set{c = C, m = M, origin = O}) ->
  {C, M, O}.

-spec state(op()) -> boolean().
state(#l_set{mem = Mem}) ->
  Mem.

-spec apply_entries(classy:site(), [lentry()], #s{}) -> {clock(), #s{}}.
apply_entries(From, Log, S0) ->
  Acked = get_acked(From, S0),
  lists:foldl(
    fun(Lentry, {_, Acc}) ->
        {Lentry#lentry.id, apply_entry(From, Lentry, Acc)}
    end,
    {Acked, S0},
    Log).

-spec apply_entry(classy:site(), lentry(), #s{}) -> #s{}.
apply_entry(
  From,
  Lentry = #lentry{ttl = TTL, payload = Op},
  S0 = #s{site = Local, last = Last0}
) ->
  #l_set{target = Target} = Op,
  case Last0 of
    #{Target := Op0} ->
      case ord(Op) > ord(Op0) of
        true ->
          Updated = true,
          Last = Last0#{Target := Op};
        false ->
          Updated = false,
          Last = Last0
      end;
    #{} ->
      Updated = true,
      Last = Last0#{Target => Op}
  end,
  S1 = S0#s{last = Last},
  S = sync_clocks(From, Lentry, S1),
  if Updated andalso From =/= Local andalso TTL > 1 ->
      log_write(Op, TTL - 1, S);
     true ->
      S
  end.

%%--------------------------------------------------------------------------------
%% Log manipulations
%%--------------------------------------------------------------------------------

-doc """
Transform a cluster management command into a log entry,
append it to the local log,
and then broadcast it to the connected peers.
""".
-spec create_and_append(join | kick, classy:site(), #s{}) -> #s{}.
create_and_append(Command, Target, S0 = #s{site = Local}) ->
  {C, S} = inc_get_clock(Target, S0),
  Op = #l_set{ origin = Local
             , target = Target
             , c = C
             , mem = case Command of
                       join -> true;
                       kick -> false
                     end
             },
  log_write(Op, ?ttl, S).

-spec log_write(op(), pos_integer(), #s{}) -> #s{}.
log_write(Op, TTL, S0 = #s{site = Local}) ->
  {Idx, S1} = inc_get_clock(Local, S0),
  Lentry = #lentry{ id = Idx
                  , ttl = TTL
                  , payload = Op
                  },
  S2 = lcons(Lentry, S1),
  S = apply_entry(Local, Lentry, S2),
  sync(false, S),
  S.

-doc """
Copy entries newer than `Acked` from the log.

Note: this function assumes that the input log is in reverse order,
but it returns items in the chronological order.
""".
-spec copy_log(clock(), [lentry()]) -> [lentry()].
copy_log(Acked, Log) ->
  copy_log(Acked, Log, []).

-spec copy_log(clock(), [lentry()], [lentry()]) -> [lentry()].
copy_log(Acked, [E | Rest], Acc) when E#lentry.id > Acked ->
  copy_log(Acked, Rest, [E | Acc]);
copy_log(_, _, Acc) ->
  Acc.

-spec lcons(lentry(), #s{}) -> #s{}.
lcons(
  Lentry = #lentry{id = Idx},
  S = #s{cbm = CBM, cbs = CBS, log = Log}
) ->
  ok = classy_rt:pset(CBM, CBS, ?cl_log, Idx, Lentry),
  S#s{log = [Lentry | Log]}.

%%--------------------------------------------------------------------------------
%% Logical clocks
%%--------------------------------------------------------------------------------

-spec sync_clocks(classy:site(), lentry(), #s{}) -> #s{}.
sync_clocks(From, #lentry{id = Cf, payload = Op}, S0) ->
  S = sync_clock(From, Cf, S0),
  #l_set{target = Target, c = Ct} = Op,
  sync_clock(Target, Ct, S).

-spec inc_get_clock(classy:site(), #s{}) -> {clock(), #s{}}.
inc_get_clock(Site, S0 = #s{clocks = Clocks}) ->
  T = maps:get(Site, Clocks, 0) + 1,
  S = S0#s{clocks = Clocks#{Site => T}},
  {T, S}.

-spec sync_clock(classy:site(), clock(), #s{}) -> #s{}.
sync_clock(Site, T1, S = #s{clocks = Clocks}) ->
  T0 = maps:get(Site, Clocks, 0),
  T = max(T0, T1),
  S#s{clocks = Clocks#{Site => T}}.

%%--------------------------------------------------------------------------------
%% Functions related to the p2p protocol
%%--------------------------------------------------------------------------------

-doc """
Try to establish connections to all known but disconnected sites.
""".
-spec reconnect(#s{}) -> #s{}.
reconnect(S0 = #s{last = Peers, site = Local, conns = Conns}) ->
  Disconnected = maps:keys(Peers) -- [Local | maps:keys(Conns)],
  %% TODO: these calls may be slow due to timeouts. Connect async-ly.
  S = lists:foldl(
        fun(Site, Acc) ->
            do_reconnect(Site, Acc)
        end,
        S0,
        Disconnected),
  erlang:send_after(?reconnect_time, self(), #cast_reconnect{}),
  S.

-spec handle_push(classy:site(), clock(), [lentry()], #s{}) -> #s{}.
handle_push(From, Since, Log, S0) ->
  Acked0 = get_acked(From, S0),
  if Acked0 >= Since ->
      {Acked, S} = apply_entries(From, Log, S0),
      ack(From, Acked, S);
     true ->
      %% Gap in the log. Ignore this message and notify the remote:
      ack(From, Acked0, S0)
  end.

-doc """
Mark `Clock` as acked for `Site` and send notification to `Site`.
""".
-spec ack(classy:site(), clock(), #s{}) -> #s{}.
ack(Site, Clock, S = #s{site = Local, conns = Conns}) ->
  case Conns of
    #{Site := #conn{pid = Pid}} ->
      gen_server:cast(Pid, #cast_ack{site = Local, acked = Clock});
    #{} ->
      ok
  end,
  set_acked(Site, Clock, S).

-spec handle_ack(classy:site(), clock(), #s{}) -> #s{}.
handle_ack(Site, Acked, S0 = #s{conns = Conns0}) ->
  case Conns0 of
    #{Site := Conn0} ->
      Conn = Conn0#conn{acked = Acked},
      Conns = Conns0#{Site := Conn},
      S = S0#s{conns = Conns},
      sync(true, Site, S);
    #{} ->
      S0
  end.

-spec do_reconnect(classy:site(), #s{}) -> #s{}.
do_reconnect(Site, S = #s{cluster = Cluster, site = Local, cbm = CBM, conns = Conns0}) ->
  case classy_rt:get_membership_pid(CBM, Cluster, Site) of
    undefined ->
      S;
    Pid when is_pid(Pid) ->
      MRef = monitor(process, Pid),
      case handshake(Pid, Local) of
        {ok, Cluster, Site, Acked} ->
          Conn = #conn{ mref = MRef
                      , pid = Pid
                      , acked = Acked
                      },
          Conns = Conns0#{Site => Conn},
          sync(false, Site, S#s{conns = Conns});
        _ ->
          demonitor(MRef),
          S
      end
  end.

-spec sync(boolean(), classy:site(), #s{}) -> #s{}.
sync(SkipEmpty, Site, S = #s{site = Local, conns = Conns, log = Log}) ->
  case Conns of
    #{Site := #conn{pid = Pid, acked = Acked}} ->
      case copy_log(Acked, Log) of
        [] when SkipEmpty ->
          ok;
        Delta ->
          gen_server:cast(Pid, #cast_push{ from = Local
                                         , since = Acked
                                         , log = Delta
                                         })
      end;
    #{} ->
      ok
  end,
  S.

-spec sync(boolean(), #s{}) -> #s{}.
sync(SkipEmpty, S = #s{conns = Conns}) ->
  maps:fold(
    fun(Site, _, Acc) ->
        sync(SkipEmpty, Site, Acc)
    end,
    S,
    Conns).

-spec get_acked(classy:site(), #s{}) -> clock().
get_acked(Site, #s{acked = Acked}) ->
  maps:get(Site, Acked, 0).

-spec set_acked(classy:site(), clock(), #s{}) -> #s{}.
set_acked(Site, Clock, S = #s{acked = Acked}) ->
  S#s{acked = Acked#{Site => Clock}}.

-spec site_of_conn(reference(), #s{}) -> {ok, classy:site()} | undefined.
site_of_conn(Ref, #s{conns = Conns}) ->
  try
    maps:foreach(
      fun(Site, #conn{mref = R}) ->
          R =:= Ref andalso throw({found, Site})
      end,
      Conns),
    undefined
  catch
    {found, Site} ->
      {ok, Site}
  end.

%%--------------------------------------------------------------------------------
%% Persistent state save/restore
%%--------------------------------------------------------------------------------

-spec restore_log(module(), classy_rt:cbs()) -> [lentry()].
restore_log(CBM, CBS) ->
  L = classy_rt:plist(CBM, CBS, ?cl_log),
  %% We need reverse chronological order:
  lists:sort(
    fun(A, B) -> B =< A end,
    L).

-spec restore_last(module(), classy_rt:cbs()) -> #{classy:site() => op()}.
restore_last(CBM, CBS) ->
  maps:from_list(classy_rt:plist(CBM, CBS, ?cl_last)).

-spec restore_clocks(module(), classy_rt:cbs()) -> #{classy:site() => clock()}.
restore_clocks(CBM, CBS) ->
  maps:from_list(classy_rt:plist(CBM, CBS, ?cl_clock)).

-spec restore_acked(module(), classy_rt:cbs()) -> #{classy:site() => clock()}.
restore_acked(CBM, CBS) ->
  maps:from_list(classy_rt:plist(CBM, CBS, ?cl_acked)).
