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
        { t :: classy_lib:wakeup_timer()
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

-spec discover_and_join() -> ok | ignore | error.
discover_and_join() ->
    with_strategy(
      fun(Mod, Options) ->
        try ekka_cluster_strategy:lock(Mod, Options) of
            ok ->
                discover_and_join(Mod, Options);
            ignore ->
                timer:sleep(rand:uniform(3000)),
                discover_and_join(Mod, Options);
            {error, Reason} ->
                logger:error("AutoCluster stopped for lock error: ~p", [Reason]),
                error
        after
            log_error("Unlock", ekka_cluster_strategy:unlock(Mod, Options))
        end
      end).

with_strategy(Fun) ->
    case application:get_env(classy, cluster_discovery) of
        {ok, {manual, _}} ->
            ignore;
        {ok, {singleton, _}} ->
            ignore;
        {ok, {Strategy, Options}} ->
            Fun(strategy_module(Strategy), Options);
        undefined ->
            ignore
    end.

strategy_module(Strategy) ->
  case code:is_loaded(Strategy) of
    {file, _} -> Strategy; %% Provider?
    false     -> list_to_atom("ekka_cluster_" ++  atom_to_list(Strategy))
  end.

-spec discover_and_join(module(), ekka_cluster_strategy:options()) -> ok | ignore | error.
discover_and_join(Mod, Options) ->
    ?tp(classy_autocluster_discover_and_join, #{mod => Mod}),
    try ekka_cluster_strategy:discover(Mod, Options) of
      {ok, Nodes} ->
        ?tp(classy_autocluster_discover_and_join_ok, #{mod => Mod, nodes => Nodes}),
        Clusters = classy:clusters(Nodes),

        Res = maybe_join(AliveNodes),
        logger:debug("join result: ~p", [Res]),
        log_error("Register", ekka_cluster_strategy:register(Mod, Options)),
        case DeadNodes of
          [] ->
            logger:info("all discovered nodes are alive", []),
            case Res of
              {error, _} -> error;
              ok         -> ok;
              ignore     -> ignore
            end;
          [_ | _] ->
            logger:info("discovered nodes are not responding: ~p", [DeadNodes]),
            error
        end;
      {error, Reason} ->
        logger:error("Discovery error: ~p", [Reason]),
        error
    catch
      _:Error:Stacktrace ->
        logger:error("Discover error: ~p~n~p", [Error, Stacktrace]),
        error
    end.

-spec maybe_join([node()]) -> ignore | ok | {error, _}.
maybe_join([]) ->
    ignore;
maybe_join(Nodes0) ->
    Nodes = lists:usort(Nodes0),
    KnownNodes = lists:usort(mria:cluster_nodes(all)),
    case Nodes =:= KnownNodes of
        true  ->
            logger:info("all discovered nodes already in cluster; ignoring", []),
            ignore;
        false ->
            OldestNode = find_oldest_mria_node(Nodes),
            logger:info("joining with ~p", [OldestNode]),
            join_with(OldestNode)
    end.

join_with(false) ->
    ignore;
join_with(Node) when Node =:= node() ->
    ignore;
join_with(Node) ->
    case classy:join_node(Node, autocluster) of
        {error, {already_in_cluster, Node}} ->
            ignore;
        Res ->
            %% Wait for ekka to be restarted after join to avoid noproc error
            %% that can occur if underlying cluster implementation (e.g. ekka_cluster_etcd)
            %% uses some processes started under ekka supervision tree
            _ = wait_application_ready(ekka, 10),
            Res
    end.

find_oldest_mria_node([Node]) ->
    Node;
find_oldest_mria_node(Nodes) ->
    case rpc:multicall(Nodes, mria_membership, local_member, [], 30000) of
        {ResL, []} ->
            case [M || M <- ResL, is_record(M, member)] of
                [] ->
                    logger:error("bad_members_found, all_nodes: ~p~n"
                                 "normal_rpc_results:~p", [Nodes, ResL]),
                    false;
                Members ->
                    (mria_membership:oldest(Members))#member.node
            end;
        {ResL, BadNodes} ->
            logger:error("bad_nodes_found, failed_nodes: ~p~n"
                         "normal_rpc_results: ~p", [BadNodes, ResL]),
            false
   end.

is_node_registered() ->
    Nodes = core_node_discovery_callback(),
    lists:member(node(), Nodes).

log_error(Format, {error, Reason}) ->
    logger:error(Format ++ " error: ~p", [Reason]);
log_error(_Format, _Ok) -> ok.

-spec wakeup(#s{}) -> #s{}.
wakeup(S) ->
  wakeup(discovery_interval(), S).

-spec wakeup(non_neg_integer(), #s{}) -> #s{}.
wakeup(After, S = #s{t = T0}) ->
  T = classy_lib:wakeup_after(#to_discover{}, After, T0),
  S#s{t = T}.

-spec discovery_interval() -> pos_integer().
discovery_interval() ->
  application:get_env(classy, discovery_interval, 5_000).
