%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A gen_server that implements automatic peer discovery.
-module(classy_autocluster).

-behavior(gen_server).

%% API:
-export([ start_link/0
        , enable/0
        , disable/0
        , decide_cluster/1
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(cast_enable, {enable :: boolean()}).
-record(to_discover, {}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec enable() -> ok.
enable() ->
  gen_server:cast(?SERVER, #cast_enable{enable = true}).

-spec disable() -> ok.
disable() ->
  gen_server:cast(?SERVER, #cast_enable{enable = false}).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { t     :: classy_lib:wakeup_timer()
        }).

init(_) ->
  process_flag(trap_exit, true),
  S = #s{},
  {ok, S}.

handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

handle_cast(#cast_enable{enable = Enable}, S0 = #s{t = T}) ->
  S = case Enable of
        true  -> wakeup(0, S0);
        false -> S0#s{t = classy_lib:cancel_wakeup(T)}
      end,
  {noreply, S};
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

handle_info(#to_discover{}, S) ->
  {noreply, handle_discover(S)};
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

terminate(Reason, _S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  ok.

%% @doc Helper function that selects a cluster from `class:cluster_info()' according to the following rules:
%%
%% <ol>
%% <li>If there are partitioned clusters, do not join.</li>
%% <li>Try to find a cluster with the largest number of peers</li>
%% <li>If the number of peers is equal in all clusters, join the cluster with the smallest ID</li>
%% </ol>
-spec decide_cluster(classy:cluster_info()) ->
        {ok, classy:cluster_id(), [{classy:site(), node()}]} |
        undefined.
decide_cluster(#{clusters := Clusters}) ->
  try
    Ret = maps:fold(
            fun(Cluster, [Sites], undefined) ->
                {Cluster, length(Sites), Sites};
               (Cluster, [Sites], {OldCluster, NOldSites, OldSites}) ->
                NSites = length(Sites),
                if NSites > NOldSites; (NSites =:= NOldSites andalso Cluster < OldCluster) ->
                    {Cluster, NSites, Sites};
                   true ->
                    {OldCluster, NOldSites, OldSites}
                end;
               (_Cluster, _, _Acc) ->
                throw(partition)
            end,
            undefined,
            Clusters),
    case Ret of
      {Cluster, _, Sites} ->
        {ok, Cluster, Sites};
      undefined ->
        undefined
    end
  catch
    partition -> undefined
  end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

handle_discover(S0) ->
  S = S0#s{t = undefined},
  discover_and_join(),
  wakeup(S).

-spec discover_and_join() -> ok | ignore | {error, _}.
discover_and_join() ->
  with_strategy(
    fun(Mod, Options) ->
        with_lock(
          Mod, Options,
          fun() ->
              maybe
                {ok, Cluster, Nodes} ?= discover(Mod, Options),
                try_join(Cluster, Nodes)
              else
                Other ->
                  log_error("Discover and join", Other),
                  ignore
              end
          end)
    end).

-spec with_lock(module(), list(), fun(() -> Ret)) -> Ret | ignore | {error, _}.
with_lock(Mod, Options, Fun) ->
  case classy_discovery_strategy:lock(Mod, Options) of
    ok ->
      try Fun()
      after
        log_error("Unlock", classy_discovery_strategy:unlock(Mod, Options))
      end;
    Other ->
      log_error("Lock", Other),
      Other
  end.

-spec discover(module(), list()) -> {ok, classy:cluster_id(), [node()]} | undefined.
discover(Mod, Options) ->
  Res = ?tp_span(debug, classy_autocluster_discover,
                 #{ mod => Mod
                  , options => Options
                  },
                 classy_discovery_strategy:discover(Mod, Options)),
  case Res of
    {ok, Candidates} ->
      Clusters = #{bad_nodes := BadNodes} = classy:clusters(Candidates),
      BadNodes =/= [] andalso
        logger:info("discovered nodes are not responding: ~p", [BadNodes]),
      case classy_hook:first_match(?on_pre_autocluster, [Candidates, Clusters]) of
        {ok, {Cluster, Nodes}} ->
          {ok, Cluster, filter_discovered_nodes(Candidates, BadNodes, Nodes)};
        _ ->
          undefined
      end;
    Other ->
      log_error("Discover", Other),
      undefined
  end.

filter_discovered_nodes(Candidates, BadNodes, Nodes) ->
  [Node || Node <- (Nodes -- BadNodes)
         , lists:member(Node, Candidates)
         ].

try_join(_Cluster, []) ->
  ignore;
try_join(Cluster, [Node | Rest]) ->
  case classy_node:join_node(Node, intent, Cluster) of
    ok ->
      ok;
    _ ->
      try_join(Cluster, Rest)
  end.

%% find_oldest_mria_node([Node]) ->
%%   Node;
%% find_oldest_mria_node(Nodes) ->
%%   case rpc:multicall(Nodes, mria_membership, local_member, [], 30000) of
%%     {ResL, []} ->
%%       case [M || M <- ResL, is_record(M, member)] of
%%         [] ->
%%           logger:error("bad_members_found, all_nodes: ~p~n"
%%                        "normal_rpc_results:~p", [Nodes, ResL]),
%%           false;
%%         Members ->
%%           (mria_membership:oldest(Members))#member.node
%%       end;
%%     {ResL, BadNodes} ->
%%       logger:error("bad_nodes_found, failed_nodes: ~p~n"
%%                    "normal_rpc_results: ~p", [BadNodes, ResL]),
%%       false
%%   end.

-spec wakeup(#s{}) -> #s{}.
wakeup(S) ->
  wakeup(discovery_interval(), S).

-spec wakeup(non_neg_integer(), #s{}) -> #s{}.
wakeup(After, S = #s{t = T0}) ->
  T = classy_lib:wakeup_after(#to_discover{}, After, T0),
  S#s{t = T}.

with_strategy(Fun) ->
    case application:get_env(classy, discovery_strategy) of
        {ok, {manual, _}} ->
            ignore;
        {ok, {singleton, _}} ->
            ignore;
        {ok, {Strategy, Options}} ->
            Fun(strategy_module(Strategy), Options);
        undefined ->
            ignore
    end.

-spec strategy_module(atom()) -> module().
strategy_module(Strategy) ->
  case code:is_loaded(Strategy) of
    {file, _} -> Strategy; %% Provider?
    false     -> list_to_atom("ekka_cluster_" ++  atom_to_list(Strategy))
  end.

-spec discovery_interval() -> pos_integer().
discovery_interval() ->
  application:get_env(classy, discovery_interval, 5_000).

log_error(Format, {error, Reason}) ->
  logger:error(Format ++ " error: ~p", [Reason]);
log_error(_Format, _Ok) ->
  ok.
