%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_membership).
-moduledoc """
# Cluster Membership CRDT

This module provides low-level API for maintaining and updating cluster membership information.
Business code should not use it directly.
""".

-behavior(gen_server).

%% API:
-export([join/4, kick/4, members/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/1]).

-export_type([start_args/0, op/0, ord/0, clock/0]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(default_sync_timeout, 100).
-define(ptab, classy_membership).

-define(name(CLUSTER, SITE), {n, l, {?MODULE, CLUSTER, SITE}}).
-define(via(CLUSTER, SITE), {via, gproc, ?name(CLUSTER, SITE)}).

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

-record(s,
        { %% Cluster ID:
          cluster :: classy:cluster_id()
          %% Local site id:
        , site :: classy:site()
          %% Runtime callback module:
        , cbm :: module()
        , sync_timer :: undefined | reference()
          %% Logical clock:
        , clock :: clock()
        }).

%% pstore keys:
-record(pk_clock, {c :: classy:cluster_id(), s :: classy:site()}).

-record(pk_acked_in, {c :: classy:cluster_id(), l :: classy:site(), r :: classy:site()}).
-record(pk_acked_out, {c :: classy:cluster_id(), l :: classy:site(), r :: classy:site()}).

-record(pk_last, {c, l, r}).
-type pk_last() :: #pk_last{c :: classy:cluster_id(), l :: classy:site(), r :: classy:site()}.

-record(pv_last, {op, mem, tou}).
-type pv_last() :: #pv_last{op :: op(), mem :: boolean(), tou :: clock()}.

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
  try
    gen_server:call(?via(Cluster, Local), #call_join{target = Target})
  catch
    EC:Err -> {error, {EC, Err}}
  end.

-doc """
Low-level call that sets `Target`'s membership state to `false`.

Note: kicking the site doesn't erase information about it.
Nodes will continue to propagate a record saying that target site is not part of the cluster.
""".
-spec kick(module(), classy:cluster_id(), classy:site(), classy:site()) -> ok.
kick(CBM, Cluster, Local, Target) ->
  try
    gen_server:call(?via(Cluster, Local), #call_kick{target = Target})
  catch
    EC:Err -> {error, {EC, Err}}
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
  MS = { #classy_pstore{ k = #pk_last{c = Cluster, l = Local, r = '$1'}
                       , v = #pv_last{mem = true, _ = '_'}
                       , _ = '_'
                       }
       , []
       , ['$1']
       },
  ets:select(?ptab, [MS]).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link(start_args()) -> {ok, pid()}.
start_link(Args = #{module := CBM, cluster := Cluster, site := Local}) when is_atom(CBM) ->
  gen_server:start_link(?via(Cluster, Local), ?MODULE, Args, []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec init(start_args()) -> {ok, #s{}}.
init(#{module := CBM, cluster := Cluster, site := Site}) ->
  process_flag(trap_exit, true),
  ok = classy_pstore:open(?ptab, #{}),
  case classy_pstore:lookup(?ptab, #pk_clock{c = Cluster, s = Site}) of
    [Clock] -> ok;
    [] -> Clock = 0
  end,
  S = #s{ cluster = Cluster
        , site = Site
        , cbm = CBM
        , clock = Clock
        },
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
handle_info(#to_sync_out{}, S0) ->
  S = S0#s{sync_timer = undefined},
  ok = classy_pstore:flush(?ptab),
  {noreply, handle_sync_out(S)};
handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, #s{}) ->
  classy_pstore:flush(?ptab).

%%================================================================================
%% Internal functions
%%================================================================================

-doc """
Total order of the operation.

Note that this total order is *not* strictly causal,
because Lamport clocks don't provide such guarantee.

Practically, lack of strict causality means the cluster will eventually converge to the same state,
but earlier join/leave commands may override later commands.

These adverse side effects can be observed when conflicting commands are issued on different nodes faster than the nodes sync with each other.
This is most likely to happen during a network partition.

Please see `theories/classy.v` for more details and some intricate requirements for `ord` function.
""".
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
      set_acked_in(From, Cf, S),
      set_acked_out(From, AckedOut, S),
      need_sync(S);
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
set_last(LTime, Op, S = #s{cluster = Cluster, site = Local}) ->
  #op_set{target = Target} = Op,
  classy_pstore:write(
    ?ptab,
    #pk_last{c = Cluster, l = Local, r = Target},
    #pv_last{op = Op, tou = LTime, mem = state(Op)}).

-spec memtab_lookup(classy:site(), #s{}) -> {ok, op()} | undefined.
memtab_lookup(Site, #s{cluster = Cluster, site = Local}) ->
  case classy_pstore:lookup(?ptab, #pk_last{c = Cluster, l = Local, r = Site}) of
    [#pv_last{op = Op}] ->
      {ok, Op};
    [] ->
      undefined
  end.

-spec memtab_since(clock(), #s{}) -> [op()].
memtab_since(Since, #s{cluster = Cluster, site = Local}) ->
  MS = { #classy_pstore{ k = #pk_last{c = Cluster, l = Local, r = '_'}
                       , v = #pv_last{op = '$1', tou = '$2', _ = '_'}
                       , _ = '_'
                       }
       , [{'>=', '$2', Since}]
       , ['$1']
       },
  ets:select(?ptab, [MS]).

-spec peers(#s{}) -> [classy:site()].
peers(#s{cluster = Cluster, site = Local}) ->
  MS = { #classy_pstore{ k = #pk_last{c = Cluster, l = Local, r = '$1'}
                       , _ = '_'
                       }
       , []
       , ['$1']
       },
  ets:select(?ptab, [MS]).

%%--------------------------------------------------------------------------------
%% Logical clocks
%%--------------------------------------------------------------------------------

-spec inc_get_clock(#s{}) -> {clock(), #s{}}.
inc_get_clock(S0 = #s{clock = C0}) ->
  C = C0 + 1,
  S = S0#s{clock = C},
  save_clock(S),
  {C, S}.

-spec sync_clock(clock(), #s{}) -> #s{}.
sync_clock(T1, S0 = #s{clock = T0}) ->
  S = S0#s{clock = max(T0, T1)},
  save_clock(S),
  S.

-spec save_clock(#s{}) -> ok.
save_clock(#s{cluster = Cluster, site = Local, clock = Clock}) ->
  classy_pstore:dirty_write(
    ?ptab,
    #pk_clock{c = Cluster, s = Local},
    Clock).

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
get_acked_in(Site, #s{cluster = C, site = Local}) ->
  case classy_pstore:lookup(?ptab, #pk_acked_in{c = C, l = Local, r = Site}) of
    [Clock] -> Clock;
    []      -> 0
  end.

-spec get_acked_out(classy:site(), #s{}) -> clock().
get_acked_out(Site, #s{cluster = C, site = Local}) ->
  case classy_pstore:lookup(?ptab, #pk_acked_out{c = C, l = Local, r = Site}) of
    [Clock] -> Clock;
    []      -> 0
  end.

-spec set_acked_in(classy:site(), clock(), #s{}) -> ok.
set_acked_in(Site, Clock, #s{cluster = Cluster, site = Local}) ->
  classy_pstore:dirty_write(
    ?ptab,
    #pk_acked_in{c = Cluster, l = Local, r = Site},
    Clock).

-spec set_acked_out(classy:site(), clock(), #s{}) -> ok.
set_acked_out(Site, Clock, #s{cluster = Cluster, site = Local}) ->
  classy_pstore:dirty_write(
    ?ptab,
    #pk_acked_out{c = Cluster, l = Local, r = Site},
    Clock).
