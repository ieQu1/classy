%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_node).

-behavior(gen_server).

%% API:
-export([start_link/0, nodes_of_cluster/1, maybe_init_the_site/2, join/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([hello/0]).

-export_type([]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

-define(ptab, classy_node).
-define(the_site, the_site).
-define(the_cluster, the_cluster).

-record(call_join, {node :: node()}).

-define(node_tab, classy_node_status_tab).

-record(node_info, {node, cluster, isup, site}).

%%================================================================================
%% API functions
%%================================================================================

-spec maybe_init_the_site(classy:cluster_id() | undefined, classy:site() | undefined) -> ok.
maybe_init_the_site(MaybeCluster, MaybeSite) ->
  ensure_value(?the_site, ?on_create_site, MaybeSite),
  ensure_value(?the_cluster, ?on_create_cluster, MaybeCluster).

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec nodes_of_cluster(classy:cluster_id()) -> #{classy:site() => node()}.
nodes_of_cluster(_Cluster) ->
  #{
   }.

-spec the_cluster() -> {ok, classy:cluster_id()} | undefined.
the_cluster() ->
  case classy_table:lookup(?ptab, ?the_cluster) of
    [V] -> {ok, V};
    []   -> undefined
  end.

-spec the_site() -> {ok, classy:site()} | undefined.
the_site() ->
  case classy_table:lookup(?ptab, ?the_site) of
    [V] -> {ok, V};
    []  -> undefined
  end.

-spec join(node()) -> ok | {error, _}.
join(Node) ->
  gen_server:call(?SERVER, #call_join{node = Node}, infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        {
        }).

init(_) ->
  process_flag(trap_exit, true),
  logger:update_process_metadata(#{domain => [classy, node]}),
  ets:new(?node_tab, [named_table, protected, {keypos, #node_info.node}]),
  net_kernel:monitor_nodes(
    true,
    #{ node_type => visible
     , nodedown_reason => true
     }),
  classy_table:open(?ptab, #{}),
  classy_hook:foreach(?on_node_init, []),
  maybe
    {ok, Cluster} ?= the_cluster(),
    {ok, Site} ?= the_site(),
    {ok, _} = classy_sup:start_membership(Cluster, Site),
    S = #s{
          },
    {ok, S}
  else
    _ ->
      {stop, default_site_not_initialized, undefined}
  end.

handle_call(#call_join{node = Node}, _From, S0) ->
  case handle_join(Node, S0) of
    {ok, S} ->
      {reply, ok, S};
    Err ->
      {reply, Err, S0}
  end;
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
  {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, S) ->
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

hello() ->
  maybe
    {ok, Cluster} ?= the_cluster(),
    {ok, Site} ?= the_site(),
    #{ site => Site
     , cluster => Cluster
     , pid => whereis(?SERVER)
     , mem_data => classy_membership:get_data(Cluster, Site, 0, 0)
     }
  else
    _ ->
      {error, not_in_cluster}
  end.

%%================================================================================
%% Internal functions
%%================================================================================

handle_join(Node, S0) ->
  case rpc:call(Node, ?MODULE, hello, [], rpc_timeout()) of
    #{ site := Remote
     , cluster := Cluster
     , pid := RemotePid
     , mem_data := MemData
     } ->
      S = monitor_site(Remote, RemotePid, S0),
      case classy_hook:all(?on_pre_join, [Cluster, Remote, Node]) of
        ok ->
          do_join_node(Node, Cluster, Remote, MemData, S);
        Err ->
          {ok, demonitor_site(Remote, Node, S)}
      end;
    Err ->
      {error, Err}
  end.

-spec do_join_node(
        node(),
        classy:cluster_id(),
        classy:site(),
        classy_membership:sync_data(),
        #s{}
       ) ->
        {ok, #s{}} | {error, _}.
do_join_node(Node, Cluster, Remote, MemData, S0) ->
  {ok, Local} = the_site(),
  case the_cluster() of
    {ok, Cluster} ->
      %% Already in the same cluster with `Node'. Just trigger
      %% re-sync (do we need to re-run hooks?):
      classy_membership:cast_sync(Cluster, Local, MemData),
      {ok, S0};
    {ok, OldCluster} when OldCluster =/= Cluster ->
      %% Site is currently in a different cluster. Leave it first:
      case leave_cluster(OldCluster, Local, S0) of
        {ok, S} ->
          do_join_node(Node, Cluster, Remote, MemData, S);
        Err ->
          Err
      end;
    undefined ->
      %% Site is not in any cluster:
      {ok, S} = join_cluster(Cluster, Local, S0),
      do_join_node(Node, Cluster, Remote, MemData, S)
  end.

leave_cluster(Cluster, Local, S) ->
  case classy_hook:all(?on_pre_leave, [Cluster, Local]) of
    ok ->
      ok = classy_hook:foreach(?on_post_leave, [Cluster, Local]),
      ok = classy_membership:set_member(Cluster, Local, Local, false),
      classy_table:delete(?ptab, ?the_cluster),
      {ok, S};
    Err ->
      Err
  end.

join_cluster(Cluster, Local, S) ->
  {ok, _} = classy_sup:start_membership(Cluster, Local),
  ok = classy_membership:set_member(Cluster, Local, Local, true),
  classy_hook:foreach(?on_post_join, [Cluster, Local]),
  set_val(?the_cluster, Cluster),
  {ok, S}.

monitor_site(Site, Node, S) ->
  S.

demonitor_site(Site, Node, S) ->
  S.

-spec ensure_value(?the_cluster | ?the_site, ?on_create_cluster | ?on_create_site, binary() | undefined) -> ok.
ensure_value(Key, OnCreateHook, Default) ->
  case classy_table:lookup(?ptab, Key) of
    [Bin] when is_binary(Bin) ->
      ok;
    [] ->
      case Default of
        undefined ->
          Val = base64:encode(crypto:strong_rand_bytes(32));
        Val when is_binary(Val) ->
          ok
      end,
      classy_hook:foreach(OnCreateHook, [Val]),
      set_val(Key, Val)
  end.

rpc_timeout() ->
  application:get_env(classy, rpc_timeout, 5_000).

set_val(Key, Val) when is_binary(Val), Key =/= ?the_site orelse Key =/= ?the_cluster ->
  classy_table:write(?ptab, Key, Val).
