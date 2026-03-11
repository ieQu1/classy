%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Cluster Membership CRDT
%%
%% This module provides low-level API for maintaining and updating cluster membership information.
%% Business code should not use it directly.
-module(classy_membership).

-behavior(gen_server).

%% API:
-export([set_member/4, members/2, list_local_sites/1, get_data/4, site_of_node/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/2, cast_sync/3]).

-export_type([start_args/0, op/0, ord/0, clock/0, sync_data/0]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-define(default_sync_timeout, 1000).
-define(ptab, classy_membership).

-define(name(CLUSTER, SITE), {n, l, {?MODULE, CLUSTER, SITE}}).
-define(via(CLUSTER, SITE), {via, gproc, ?name(CLUSTER, SITE)}).

-type start_args() ::
        #{ cluster := classy:cluster_id()
         , site    := classy:site()
         }.

-type clock() :: non_neg_integer().

-define(mem, mem).
-define(host, host).

-type site_prop() :: ?mem   %% Is member?
                   | ?host. %% Which node hosts the site

%% Arbitrary term used to break ties between commands with the same logical timestamp.
-type magic() :: term().

%% The following command is used to update status of `target' site:
-record(op_set, {origin, target, k, c, m, val, owt}).

-type op() ::
        #op_set{ origin :: classy:site() %% Site that issued the command
               , target :: classy:site() %% Site which is being updated
               , k :: site_prop() %% Property of the target site that is being updated
               , c :: clock() %% Logical time at `origin' when it issued the command:
               , m :: magic() %% This term can be used to break ties
               , val :: term() %% Updated value
               , owt :: integer() %% Origin's wall time when the update was made. This value
                                  %% isn't used by this module directly, but it gives hints to
                                  %% the autoclean:
               }.

%% Projection of `op()` fields used to establish total order of logs.
-type ord() :: {clock(), magic(), classy:site()}.

%% `site' sends a portion of its logs that is newer than `since':
-record(cast_sync,
        { cluster :: classy:cluster_id()
        , from :: classy:site()
        , since :: clock()
        , acked :: clock()
          %% c
        , c :: clock()
        , data :: [op()]
        , reserved
        }).

-type sync_data() :: #cast_sync{}.

%% Timeout message triggering syncing out state
-record(to_sync_out, {}).

-record(call_set, {target :: classy:site(), k :: site_prop(), v :: term()}).
-record(call_get_data, {since :: clock(), acked :: clock()}).

-record(s,
        { %% Cluster ID:
          cluster :: classy:cluster_id()
          %% Local site id:
        , site :: classy:site()
        , sync_timer :: undefined | reference()
          %% Logical clock:
        , clock :: clock()
        }).

%% Table keys:
-record(pk_clock,
        { c %% Cluster
        , s %% Site (local)
        }).
-record(pk_acked_in, {c :: classy:cluster_id(), l :: classy:site(), r :: classy:site()}).
-record(pk_acked_out, {c :: classy:cluster_id(), l :: classy:site(), r :: classy:site()}).
-record(pk_last, {c, l, r, k}).
-type pk_last() :: #pk_last{c :: classy:cluster_id(), l :: classy:site(), r :: classy:site(), k :: site_prop()}.

%% Composite table values:
-record(pv_last, {op, tou, hooks_ran = false}).
-type pv_last() :: #pv_last{op :: op(), tou :: clock(), hooks_ran :: boolean()}.

%%================================================================================
%% API functions
%%================================================================================

%% Low-level call that sets `Target''s membership state to `true'.
%%
%% WARNING: this function does not check if target site exists and/or is part of cluster.
%% When called with invalid `Target',
%% it will create a new entry that will eventually make its way to the entire cluster.
%% This fictitious site will exist in `down` state until kicked,
%% and even then some records about it may be kept around.
-spec set_member(classy:cluster_id(), classy:site(), classy:site(), boolean()) -> ok | {error, _}.
set_member(Cluster, Local, Target, Mem) when is_boolean(Mem) ->
  try
    gen_server:call(?via(Cluster, Local), #call_set{target = Target, k = ?mem, v = Mem}, infinity)
  catch
    EC:Err -> {error, {EC, Err}}
  end.

%% @doc Return active members of the `Cluster`,
%% as perceived by `Local` site.
%%
%% WARNING: if `Local` is not a member of the returned list,
%% then the local system may be permanently out of sync with the `Cluster` or `{Cluster,Local}` server may be inactive.
%% In both cases the result value can't be trusted.
-spec members(classy:cluster_id(), classy:site()) -> [classy:site()].
members(Cluster, Local) ->
  MS = { #classy_kv{ k = #pk_last{c = Cluster, l = Local, r = '$1', k = mem}
                   , v = #pv_last{op = #op_set{val = true, _ = '_'}, _ = '_'}
                   , _ = '_'
                   }
       , []
       , ['$1']
       },
  ets:select(?ptab, [MS]).

-spec list_local_sites(running | all) -> [{classy:cluster_id(), classy:site()}].
list_local_sites(running) ->
  MS = {{?name('$1', '$2'), '_', '_'}, [], [{{'$1', '$2'}}]},
  gproc:select({local, names}, [MS]);
list_local_sites(all) ->
  %% Every local site has a logical clock:
  MS = { #classy_kv{ k = #pk_clock{c = '$1', s = '$2'}
                   , _ = '_'
                   }
       , []
       , [{{'$1', '$2'}}]
       },
  ets:select(?ptab, [MS]).

-spec site_of_node(classy:cluster_id(), classy:site()) -> #{node() => classy:site()}.
site_of_node(Cluster, Local) ->
  maps:from_list(select_nodes(Cluster, Local, {{'$2', '$1'}})).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link(classy:cluster_id(), classy:site()) -> {ok, pid()}.
start_link(Cluster, Local) ->
  Args = #{cluster => Cluster, site => Local},
  gen_server:start_link(?via(Cluster, Local), ?MODULE, Args, []).

-spec cast_sync(classy:cluster_id(), classy:site(), sync_data()) -> ok.
cast_sync(Cluster, Site, Cast) ->
  gen_server:cast(?via(Cluster, Site), Cast).

-spec get_data(classy:cluster_id(), classy:site(), clock(), clock()) -> sync_data().
get_data(Cluster, Local, Since, Acked) ->
  gen_server:call(?via(Cluster, Local), #call_get_data{since = Since, acked = Acked}).

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec init(start_args()) -> {ok, #s{}}.
init(#{cluster := Cluster, site := Site}) when is_binary(Site), is_binary(Cluster) ->
  process_flag(trap_exit, true),
  logger:update_process_metadata(
    #{ domain => [classy, membership]
     , cluster => Cluster
     , local => Site
     }),
  ok = classy_table:open(?ptab, #{}),
  case classy_table:lookup(?ptab, #pk_clock{c = Cluster, s = Site}) of
    [Clock] -> ok;
    [] -> Clock = 0
  end,
  S0 = #s{ cluster = Cluster
         , site = Site
         , clock = Clock
         },
  %% Establish own membership in the cluster. The below operation
  %% (with clock = 0) is used to set the default value.
  S1 = local_command(
         0,
         #call_set{ target = Site
                  , k = ?mem
                  , v = true
                  },
         S0),
  S = local_command(
        #call_set{ target = Site
                 , k = ?host
                 , v = node()
                 },
        S1),
  {ok, need_sync(0, S)}.

handle_call(#call_set{} = CMD, _From, S0) ->
  S = local_command(CMD, S0),
  {reply, ok, S};
handle_call(#call_get_data{since = Since, acked = Acked}, _From, S) ->
  {reply, get_sync_data(Since, Acked, S), S};
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

handle_cast(#cast_sync{} = Req, S0) ->
  S = handle_sync_in(Req, S0),
  run_hooks(S),
  {noreply, S};
handle_cast(_Cast, S) ->
  {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(#to_sync_out{}, S0) ->
  S1 = S0#s{sync_timer = undefined},
  ok = classy_table:flush(?ptab),
  S = handle_sync_out(S1),
  run_hooks(S),
  {noreply, need_sync(S)};
handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, #s{}) ->
  classy_table:flush(?ptab).

%%================================================================================
%% Internal functions
%%================================================================================

%% @doc Total order of the operation.
%%
%% Note that this total order is *not* strictly causal,
%% because Lamport clocks don't provide such guarantee.
%%
%% Practically, lack of strict causality means the cluster will eventually converge to the same state,
%% but earlier join/leave commands may override later commands.
%%
%% These adverse side effects can be observed when conflicting commands are issued on different nodes faster than the nodes sync with each other.
%% This is most likely to happen during a network partition.
%%
%% Please see `theories/classy.v` for more details and some intricate requirements for `ord` function.
-spec ord(op()) -> ord().
ord(#op_set{c = C, m = M, origin = O}) ->
  {C, M, O}.

-spec state(op()) -> boolean().
state(#op_set{val = Val}) ->
  Val.

-spec local_command(#call_set{}, #s{}) -> #s{}.
local_command(Cmd, S0) ->
  {C, S} = inc_get_clock(S0),
  local_command(C, Cmd, S).

-spec local_command(clock(), #call_set{}, #s{}) -> #s{}.
local_command(C, #call_set{target = Target, k = K, v = V}, S = #s{site = Local}) ->
  ?tp(classy_local_command,
      #{ target => Target
       , prop => K
       , val => V
       , clock => C
       }),
  Op = #op_set{ origin = Local
              , target = Target
              , c = C
              , k = K
              , val = V
              , owt = time_s()
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
  ?tp(debug, classy_membership_sync_in,
      #{ from => From
       , since => Since
       , clock => Cf
       , acked => AckedOut
       , data => Data
       }),
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
handle_sync_out(S = #s{cluster = Cluster}) ->
  SyncTargets = sync_targets(S),
  ?tp(debug, classy_membership_sync_out,
      #{ targets => SyncTargets
       }),
  maps:foreach(
    fun(Site, Node) ->
        Since = get_acked_out(Site, S),
        Acked = get_acked_in(Site, S),
        Data = get_sync_data(Since, Acked, S),
        ?tp(debug, classy_membership_sync_target,
            #{ remote => Site
             , noe => Node
             , since => Since
             , acked => Acked
             , data => Data
             }),
        case node() of
          Node -> ?MODULE:cast_sync(Cluster, Site, Data);
          _    -> erpc:cast(Node, ?MODULE, cast_sync, [Cluster, Site, Data])
        end
    end,
    SyncTargets),
  S.

get_sync_data(Since, Acked, S = #s{cluster = Cluster, site = Local, clock = C}) ->
  #cast_sync{ cluster = Cluster
            , from = Local
            , since = Since
            , acked = Acked
            , c = C
            , data = memtab_since(Since, S)
            }.

-spec merge(clock(), op(), #s{}) -> boolean().
merge(LTime, Op, S) ->
  #op_set{target = Site, k = K} = Op,
  case memtab_lookup(Site, K, S) of
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

run_hooks(S = #s{cluster = Cluster, site = Local}) ->
  lists:foreach(
    fun(Peer) ->
        K = #pk_last{c = Cluster, l = Local, r = Peer, k = mem},
        case classy_table:lookup(?ptab, K) of
          [#pv_last{hooks_ran = true}] ->
            ok;
          [#pv_last{op = Op, hooks_ran = false} = V] ->
            classy_hook:foreach(?on_membership_change, [Cluster, Local, Peer, state(Op)]),
            classy_table:dirty_write(?ptab, K, V#pv_last{hooks_ran = true})
        end
    end,
    peers(S)).

%%--------------------------------------------------------------------------------
%% Interface for site state storage
%%--------------------------------------------------------------------------------

-spec set_last(clock(), op(), #s{}) -> ok.
set_last(LTime, Op, #s{cluster = Cluster, site = Local}) ->
  #op_set{target = Target, k = K} = Op,
  classy_table:dirty_write(
    ?ptab,
    #pk_last{c = Cluster, l = Local, r = Target, k = K},
    #pv_last{op = Op, tou = LTime}).

-spec memtab_lookup(classy:site(), site_prop(), #s{}) -> {ok, op()} | undefined.
memtab_lookup(Site, K, #s{cluster = Cluster, site = Local}) ->
  case classy_table:lookup(?ptab, #pk_last{c = Cluster, l = Local, r = Site, k = K}) of
    [#pv_last{op = Op}] ->
      {ok, Op};
    [] ->
      undefined
  end.

-spec memtab_since(clock(), #s{}) -> [op()].
memtab_since(Since, #s{cluster = Cluster, site = Local}) ->
  MS = { #classy_kv{ k = #pk_last{c = Cluster, l = Local, _ = '_'}
                   , v = #pv_last{op = '$1', tou = '$2', _ = '_'}
                   , _ = '_'
                   }
       , [{'>=', '$2', Since}]
       , ['$1']
       },
  ets:select(?ptab, [MS]).

-spec peers(#s{}) -> [classy:site()].
peers(#s{cluster = Cluster, site = Local}) ->
  MS = { #classy_kv{ k = #pk_last{c = Cluster, l = Local, r = '$1', k = ?mem}
                   , _ = '_'
                   }
       , []
       , ['$1']
       },
  ets:select(?ptab, [MS]).

-spec nodes_of_cluster(#s{}) -> #{classy:site() => node()}.
nodes_of_cluster(#s{cluster = Cluster, site = Local}) ->
  maps:from_list(select_nodes(Cluster, Local, {{'$1', '$2'}})).

select_nodes(Cluster, Local, Action) ->
  MS = { #classy_kv{ k = #pk_last{c = Cluster, l = Local, r = '$1', k = ?host}
                   , v = #pv_last{op = #op_set{val = '$2', _ = '_'}, _ = '_'}
                   , _ = '_'
                   }
       , []
       , [Action]
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
  classy_table:dirty_write(
    ?ptab,
    #pk_clock{c = Cluster, s = Local},
    Clock).

%%--------------------------------------------------------------------------------
%% Functions related to the p2p protocol
%%--------------------------------------------------------------------------------

%% Schedule sync with the peers after the default timeout.
-spec need_sync(#s{}) -> #s{}.
need_sync(S) ->
  need_sync(
    application:get_env(classy, sync_timeout, ?default_sync_timeout),
    S).

%% Schedule sync with the peers after a specified timeout.
%% This call has no effect if the sync is already scheduled.
-spec need_sync(non_neg_integer(), #s{}) -> #s{}.
need_sync(_Timeout, S = #s{sync_timer = R}) when is_reference(R) ->
  S;
need_sync(Timeout, S = #s{sync_timer = undefined}) ->
  TRef = erlang:send_after(Timeout, self(), #to_sync_out{}),
  S#s{sync_timer = TRef}.

-spec get_acked_in(classy:site(), #s{}) -> clock().
get_acked_in(Site, #s{cluster = C, site = Local}) ->
  case classy_table:lookup(?ptab, #pk_acked_in{c = C, l = Local, r = Site}) of
    [Clock] -> Clock;
    []      -> 0
  end.

-spec get_acked_out(classy:site(), #s{}) -> clock().
get_acked_out(Site, #s{cluster = C, site = Local}) ->
  case classy_table:lookup(?ptab, #pk_acked_out{c = C, l = Local, r = Site}) of
    [Clock] -> Clock;
    []      -> 0
  end.

-spec set_acked_in(classy:site(), clock(), #s{}) -> ok.
set_acked_in(Site, Clock, #s{cluster = Cluster, site = Local}) ->
  classy_table:dirty_write(
    ?ptab,
    #pk_acked_in{c = Cluster, l = Local, r = Site},
    Clock).
-spec set_acked_out(classy:site(), clock(), #s{}) -> ok.
set_acked_out(Site, Clock, #s{cluster = Cluster, site = Local}) ->
  classy_table:dirty_write(
    ?ptab,
    #pk_acked_out{c = Cluster, l = Local, r = Site},
    Clock).

sync_targets(S = #s{cluster = Cluster, site = Local}) ->
  maps:remove(
    Local,
    maps:merge(
      nodes_of_cluster(S),
      classy_node:nodes_of_cluster(Cluster)
     )).

-ifndef(CONCUERROR).

time_s() ->
  os:system_time(second).

-endif.

%%--------------------------------------------------------------------------------
%% Unit tests
%%--------------------------------------------------------------------------------

-ifdef(TEST).

table_scans_test() ->
  Cleanup = classy_table_tests:setup(?FUNCTION_NAME),
  S1 = #s{cluster = <<"c1">>, site = <<"s1">>},
  S2 = #s{cluster = <<"c2">>, site = <<"s2">>},
  try
    classy_table:open(?ptab, #{}),
    [begin
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , target = <<"s1">>
                       , k = ?mem
                       , val = true
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , target = <<"s1">>
                       , k = ?host
                       , val = 'n1@localhost'
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , target = <<"s2">>
                       , k = ?mem
                       , val = false
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , target = <<"s2">>
                       , k = ?host
                       , val = 'n2@localhost'
                       },
                S)
     end || S <- [S1, S2]],
    %% Check `peers' function:
    [?assertEqual(
        [<<"s1">>, <<"s2">>],
        lists:sort(peers(S)))
     || S <- [S1, S2]],
    %% Check `members' function:
    ?assertEqual(
       [<<"s1">>],
       members(<<"c1">>, <<"s1">>)),
    ?assertEqual(
       [<<"s1">>],
       members(<<"c2">>, <<"s2">>)
      ),
    ?assertEqual(
       [],
       members(<<"c1">>, <<"s2">>)),
    %% Check `nodes_of_cluster' function:
    [?assertEqual(
        #{<<"s1">> => 'n1@localhost', <<"s2">> => 'n2@localhost'},
        nodes_of_cluster(S))
     || S <- [S1, S2]]
  after
    classy_table:drop(?ptab),
    classy_table_tests:cleanup(Cleanup)
  end.

-endif.
