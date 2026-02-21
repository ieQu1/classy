%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_membership).
-moduledoc """
# Cluster Membership CRDT

This module provides low-level API for maintaining and updating cluster membership information.
Business code should not use it directly.

# Logs

Each site `s` maintains a command log `L[s]`.
When a command (such as `join(site)` or `leave(site)`) is executed on a site,
it is added to the log.

Sites constantly try to broadcast their logs to known peers.

# Logical Clocks

Each site is associated with a Lamport clock.
Value of the clock of site `a`, as viewed on site `b`, is denoted as `C[a, b]`.

Clocks are updated in the following cases:

- `C[a, a]` is incremented when site `a` appends a new command to `L[a]`.
  Entries in `L[a]` are indexed by `C[a,a]`.
- `C[a, b]` is incremented when `b` appends a command concerning `a` to `L[b]`.
  New value of the clock is added to the command body.
- `C[a, b]` is synced when `b` receives a message from or about `a`.

# Event ordering

Eventual consistency is assured by the existence of a total order of log entries.
Provided that each site received all log entries from all peers (in any order),
it derives peer membership states from the log entries of the maximum order.

More formally,
let `S[a, b]` be state of site `a` as perceived by `b`,
and `L` be a concatenation of all logs from all sites,
then `S[a, b] = maximum(fun event_order/2, filter(concerning_site(b), L))`.

If comparison function `event_order/2` is transitive,
then the above expression returns the same value for every permutation of `L`.
This module uses lexicographic order of `{C[a, b], M, b}` triples as the event order.
Lexicographic order is known to be well-behaved.


Note that this total order is *not* causal.
Practically, it means that cluster will eventually converge to the same state,
but earlier join/leave commands may override later commands.

These adverse side effects can be observed when conflicting commands are issued on different nodes faster than both nodes can sync with each other.
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

-export_type([cluster_id/0, site/0, start_args/0, peer_state/0, event_order/0, clock/0]).

-include("classy.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(reconnect_time, 1_000).
-define(ttl, 5).

-type start_args() ::
        #{ module  := module()
         , cluster := cluster_id()
         , site    := site()
         }.

-type cluster_id() :: binary().

-type site() :: binary().

-type clock() :: non_neg_integer().

-doc """
Arbitrary term used to break ties between commands with the same logical timestamp.
""".
-type magic() :: term().

-doc """
The following two commands are used to update cluster membership of `site`.
""".
-record(l_join, {site :: site(), c :: clock(), m :: magic(), ttl :: pos_integer()}).
-record(l_kick, {site :: site(), c :: clock(), m :: magic(), ttl :: pos_integer()}).

-type op() :: #l_join{}
            | #l_kick{}.

-type lentry() :: {clock(), op()}.

-type event_order() :: {clock(), magic(), site()}.

-opaque peer_state() :: #member_s_v0{}.

-type peers() :: #{site() => peer_state()}.

-record(call_handshake, {from :: site()}).
%% Server periodically sends this message to itself to refresh
%% connections that went down:
-record(cast_reconnect, {}).

%% `site' sends a portion of its logs that is newer than `since':
-record(cast_push,
        { from :: site()
        , since :: clock()
        , log :: [lentry()]
        }).
%% Set acked log entry for `site' to `acked':
-record(cast_ack,
        { site :: site()
        , acked :: clock()
        }).

-record(call_join, {target :: site()}).
-record(call_kick, {target :: site()}).

-record(conn,
        { mref :: reference()
        , pid :: pid()
        , acked :: clock()
        }).

-record(s,
        { %% Cluster cookie:
          cluster :: cluster_id()
          %% My site id:
        , site :: site()
          %% Callback module:
        , cbm :: module()
          %% State of the callback module:
        , cbs :: cbs()
          %% Log of outgoing commands. Log is stored in reverse
          %% chronological order, with newer entries closer to the
          %% head.
        , log :: [lentry()]
          %% Collection of peer states derived by merging incoming and
          %% outgoing the logs:
        , peers :: peers()
        , conns = #{} :: #{site() => #conn{}}
        }).

%%================================================================================
%% Behavior definition:
%%================================================================================

-doc """
State of the callback module.
It is opaque to us.
""".
-type cbs() :: term().

-callback classy_init(cluster_id(), site()) -> {ok, cbs()}.

-callback classy_terminate(cbs()) -> _.

-doc """
Get PID of a potentially remote `classy_membership` server.
""".
-callback classy_get_pid(cluster_id(), site()) -> pid() | undefined.

-doc """
Store a key-value pair persistently.
""".
-callback classy_pset(cbs(), log, clock(), op()) -> ok;
                     (cbs(), peer, site(), peer_state()) -> ok.

-doc """
Delete a key-value pair persistently.
""".
-callback classy_pdel(cbs(), log, clock()) -> ok;
                     (cbs(), peer, site()) -> ok.

-doc """
List persistent values.
""".
-callback classy_plist(cbs(), log) -> [lentry()];
                      (cbs(), peer) -> [{site(), peer_state()}].

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
-spec join(module(), cluster_id(), site(), site()) -> ok | {error, _}.
join(CBM, Cluster, Local, Target) ->
  case cbm_get_pid(CBM, Cluster, Local) of
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
-spec kick(module(), cluster_id(), site(), site()) -> ok.
kick(CBM, Cluster, Local, Target) ->
  case cbm_get_pid(CBM, Cluster, Local) of
    Pid when is_pid(Pid) ->
      try
        gen_server:call(Pid, #call_kick{target = Target})
      catch
        EC:Err -> {error, {EC, Err}}
      end;
    undefined ->
      {error, noproc}
  end.

-spec members(cluster_id(), site()) -> [site()].
members(Cluster, Local) ->
  error(todo).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link(start_args()) -> {ok, pid()}.
start_link(Args = #{module := CBM, cluster := _, site := _}) when is_atom(CBM) ->
  gen_server:start_link(?MODULE, Args, []).

-spec handshake(pid(), site()) -> {ok, cluster_id(), site(), clock()} | {error, _}.
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
  {ok, CBS} = cbm_init(CBM, Cluster, Site),
  Peers = restore_peers(CBM, CBS),
  S = #s{ cluster = Cluster
        , site = Site
        , cbm = CBM
        , cbs = CBS
        , log = restore_log(CBM, CBS)
        , peers = Peers
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
  cbm_terminate(CBM, CBS).

%%================================================================================
%% Internal functions
%%================================================================================

-doc """
Transform a cluster management command into a log entry,
append it to the local log,
and then broadcast it to the connected peers.
""".
-spec create_and_append(join | kick, site(), #s{}) -> #s{}.
create_and_append(Command, Target, S0) ->
  {C, S} = inc_get_clock(Target, S0),
  Op = case Command of
         join -> #l_join{site = Target, c = C, ttl = ?ttl};
         kick -> #l_kick{site = Target, c = C, ttl = ?ttl}
       end,
  log_write(Op, S).

-spec log_write(op(), #s{}) -> #s{}.
log_write(Op, S0 = #s{site = Local}) ->
  {Idx, S1} = inc_get_clock(Local, S0),
  Lentry = {Idx, Op},
  S2 = lcons(Lentry, S1),
  S = apply_entry(Local, Lentry, S2),
  sync(false, S),
  S.

-doc """
Try to establish connections to all known but disconnected sites.
""".
-spec reconnect(#s{}) -> #s{}.
reconnect(S0 = #s{peers = Peers, site = Local, conns = Conns}) ->
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

-spec handle_push(site(), clock(), [lentry()], #s{}) -> #s{}.
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
-spec ack(site(), clock(), #s{}) -> #s{}.
ack(Site, Clock, S = #s{site = Local, conns = Conns}) ->
  case Conns of
    #{Site := #conn{pid = Pid}} ->
      gen_server:cast(Pid, #cast_ack{site = Local, acked = Clock});
    #{} ->
      ok
  end,
  set_acked(Site, Clock, S).

-spec handle_ack(site(), clock(), #s{}) -> #s{}.
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

-spec do_reconnect(site(), #s{}) -> #s{}.
do_reconnect(Site, S = #s{cluster = Cluster, site = Local, cbm = CBM, conns = Conns0}) ->
  case cbm_get_pid(CBM, Cluster, Site) of
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

-spec sync(boolean(), site(), #s{}) -> #s{}.
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

-spec apply_entries(site(), [lentry()], #s{}) -> {clock(), #s{}}.
apply_entries(From, Log, S0) ->
  Acked = get_acked(From, S0),
  lists:foldl(
    fun(Lentry = {Idx, _}, {_, Acc}) ->
        {Idx, apply_entry(From, Lentry, Acc)}
    end,
    {Acked, S0},
    Log).

-spec apply_entry(site(), lentry(), #s{}) -> #s{}.
apply_entry(From, {Idx, Op}, S0 = #s{peers = Peers0}) ->
  {Updated, Peers} = update_peer(From, Op, Peers0),
  S = S0#s{peers = Peers},
  sync_clock(From, Idx, S).

-spec update_peer(site(), op(), peers()) -> {boolean(), peers()}.
update_peer(From, Op, Peers) ->
  case Op of
    #l_join{c = C, site = S, m = Magic} ->
      Mem = true;
    #l_kick{c = C, site = S, m = Magic} ->
      Mem = false
  end,
  Ord = {C, Magic, From},
  case Peers of
    #{S := PS0} ->
      #member_s_v0{ord = Ord0, ts = C0} = PS0,
      case Ord0 < Ord of
        true ->
          PS = PS0#member_s_v0{ ord = Ord
                              , mem = Mem
                              , ts = max(C0, C)
                              },
          {true, Peers#{S := PS}};
        false ->
          {false, Peers}
      end;
    #{} ->
      PS = #member_s_v0{ord = Ord, mem = Mem, ts = C},
      {true, Peers#{S => PS}}
  end.

-spec inc_get_clock(site(), #s{}) -> {clock(), #s{}}.
inc_get_clock(Site, S0 = #s{peers = Peers0}) ->
  PS0 = maps:get(Site, Peers0, #member_s_v0{}),
  TS = PS0#member_s_v0.ts + 1,
  PS = PS0#member_s_v0{ts = TS},
  Peers = Peers0#{Site => PS},
  S = S0#s{peers = Peers},
  {TS, S}.

-spec sync_clock(site(), clock(), #s{}) -> #s{}.
sync_clock(Site, TS1, S = #s{peers = Peers0}) ->
  PS0 = maps:get(Site, Peers0, #member_s_v0{}),
  TS = max(PS0#member_s_v0.ts, TS1),
  PS = PS0#member_s_v0{ts = TS},
  Peers = Peers0#{Site => PS},
  S#s{peers = Peers}.

-doc """
Copy entries newer than `Acked` from the log.

Note: this function assumes that the input log is in reverse order,
but it returns items in the chronological order.
""".
-spec copy_log(clock(), [lentry()]) -> [lentry()].
copy_log(Acked, Log) ->
  copy_log(Acked, Log, []).

-spec copy_log(clock(), [lentry()], [lentry()]) -> [lentry()].
copy_log(Acked, [E = {Clock, _} | Rest], Acc) when Clock > Acked ->
  copy_log(Acked, Rest, [E | Acc]);
copy_log(_, _, Acc) ->
  Acc.

-spec get_acked(site(), #s{}) -> clock().
get_acked(Site, #s{peers = Peers}) ->
  case Peers of
    #{Site := #member_s_v0{ii = C}} ->
      C;
    #{} ->
      0
  end.

-spec set_acked(site(), clock(), #s{}) -> #s{}.
set_acked(Site, Clock, S = #s{peers = Peers0}) ->
  case Peers0 of
    #{Site := PS0} ->
      PS = PS0#member_s_v0{ii = Clock},
      Peers = Peers0#{Site := PS},
      S#s{peers = Peers};
    #{} ->
      S
  end.

-spec lcons(lentry(), #s{}) -> #s{}.
lcons(Lentry = {Idx, Op}, S = #s{cbm = CBM, cbs = CBS, log = Log}) ->
  ok = cbm_pset(CBM, CBS, log, Idx, Op),
  S#s{log = [Lentry | Log]}.

-spec site_of_conn(reference(), #s{}) -> {ok, site()} | undefined.
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

-spec restore_log(module(), cbs()) -> [lentry()].
restore_log(CBM, CBS) ->
  L = cbm_plist(CBM, CBS, log),
  %% We need reverse chronological order:
  lists:sort(
    fun(A, B) -> B =< A end,
    L).

-spec restore_peers(module(), cbs()) -> #{site() => peer_state()}.
restore_peers(CBM, CBS) ->
  maps:from_list(cbm_plist(CBM, CBS, peer)).

-spec cbm_init(module(), cluster_id(), site()) -> {ok, cbs()}.
cbm_init(Mod, Cluster, Site) ->
  Mod:classy_init(Cluster, Site).

-spec cbm_terminate(module(), cbs()) -> ok.
cbm_terminate(Mod, CBS) ->
  Mod:classy_terminate(CBS),
  ok.

-spec cbm_get_pid(module(), cluster_id(), site()) -> pid() | undefined.
cbm_get_pid(Mod, Cluster, Site) ->
  Mod:classy_get_pid(Cluster, Site).

-spec cbm_pset(module(), cbs(), log, clock(), op()) -> ok;
              (module(), cbs(), peer, site(), peer_state()) -> ok.
cbm_pset(Mod, CBS, Kind, K, V) ->
  Mod:classy_pset(CBS, Kind, K, V).

-spec cbm_pdel(module(), cbs(), log, clock()) -> ok;
              (module(), cbs(), peer, site()) -> ok.
cbm_pdel(Mod, CBS, Kind, K) ->
  Mod:classy_pdel(CBS, Kind, K).

-spec cbm_plist(module(), cbs(), log) -> [lentry()];
               (module(), cbs(), peer) -> [{site(), peer_state()}].
cbm_plist(Mod, CBS, Kind) ->
  Mod:classy_plist(CBS, Kind).
