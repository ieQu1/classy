%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_node).

-behavior(gen_server).

%% API:
-export([ start_link/0
        , maybe_init_the_site/2
        , join_node/2
        , kick_site/2
        , the_site/0
        , the_cluster/0
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([hello/0]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

-define(ptab, classy_node).
-define(the_site, the_site).
-define(the_cluster, the_cluster).

-record(call_join, {node :: node(), intent :: term()}).
-record(call_kick, {site :: classy:site(), intent :: term()}).
-record(cast_membership_change,
        { cluster :: classy:cluster_id()
        , local :: classy:site()
        , remote :: classy:site()
        , member :: boolean()
        }).

-define(node_tab, classy_node_status_tab).

-record(node_info, {node, cluster, isup, site}).

-type run_level_int() :: 0..3.
-type run_level_atom() :: ?stopped | ?single | ?cluster | ?quorum.

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

-spec join_node(node(), _Intent) -> ok | {error, _}.
join_node(Node, Intent) ->
  gen_server:call(
    ?SERVER,
    #call_join{node = Node, intent = Intent},
    infinity).

-spec kick_site(classy:site(), _Intent) -> ok | {error, _}.
kick_site(Site, Intent) ->
  gen_server:call(
    ?SERVER,
    #call_kick{site = Site, intent = Intent},
    infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { cluster :: classy:cluster_id() | undefined
        , site :: classy:site()
        , run_level = 0 :: run_level_int()
        }).

init(_) ->
  process_flag(trap_exit, true),
  %% logger:update_process_metadata(#{domain => [classy, node]}),
  ets:new(?node_tab, [named_table, protected, {keypos, #node_info.node}]),
  net_kernel:monitor_nodes(
    true,
    #{ node_type => visible
     , nodedown_reason => true
     }),
  classy_table:open(?ptab, #{}),
  classy_hook:foreach(?on_node_init, []),
  classy:on_membership_change(fun on_membership_change/4, -100),
  maybe
    {ok, Cluster} ?= the_cluster(),
    {ok, Site} ?= the_site(),
    {ok, _} = classy_sup:ensure_membership(Cluster, Site),
    S = adjust_run_level(
          #s{ cluster = Cluster
            , site = Site
            }),
    {ok, S}
  else
    _ ->
      {stop, default_site_not_initialized, undefined}
  end.

handle_call(#call_join{node = Node, intent = Intent}, _From, S0) ->
  case handle_join(Node, S0, Intent) of
    {ok, S} ->
      {reply, ok, S};
    Err ->
      {reply, Err, S0}
  end;
handle_call(#call_kick{site = Target, intent = Intent}, _From, S) ->
  Ret =
    maybe
      {ok, Cluster} ?= the_cluster(),
      {ok, Local} ?= the_site(),
      handle_kick(Cluster, Local, Target, Intent)
    else
      _ -> {error, local_not_in_cluster}
    end,
  {reply, Ret, S};
handle_call(Call, From, S) ->
  ?tp(warning, classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

handle_cast(#cast_membership_change{} = Cast, S) ->
  {noreply, handle_membership_change_event(Cast, S)};
handle_cast(Cast, S) ->
  ?tp(warning, classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

handle_info({NodeUpOrDown, _Node, _}, S) when NodeUpOrDown =:= nodeup; NodeUpOrDown =:= nodedown ->
  {noreply, adjust_run_level(S)};
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
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

on_membership_change(Cluster, Local, Remote, Member) ->
  gen_server:cast(?SERVER,
                  #cast_membership_change{ cluster = Cluster
                                         , local = Local
                                         , remote = Remote
                                         , member = Member
                                         }).

handle_membership_change_event(
  #cast_membership_change{ cluster = Cluster
                         , local = Local
                         , remote = Remote
                         , member = Member
                         },
  S = #s{cluster = ThisCluster, site = ThisSite}
 ) ->
  ?tp(warning, membership_change,
      #{ cluster => Cluster
       , origin => Local
       , target => Remote
       , member => Member
       }),
  if Cluster =:= ThisCluster,
     Local =:= ThisSite,
     Remote =:= ThisSite,
     Member =:= false ->
      %% We got kicked:
      erlang:display(we_got_kicked),
      ?tp(warning, classy_kicked_remotely,
          #{ cluster => Cluster
           }),
      on_leave(S, kicked);
     Cluster =:= ThisCluster ->
      adjust_run_level(S);
     true ->
      S
  end.

handle_kick(Cluster, Local, Target, Intent) ->
  case classy_hook:all(?on_pre_kick, [Cluster, Target, Intent]) of
    ok ->
      Ret = classy_membership:set_member(Cluster, Local, Target, false),
      classy_membership:flush(Cluster, Local),
      Ret;
    Err ->
      Err
  end.

handle_join(Node, S, Intent) ->
  case rpc:call(Node, ?MODULE, hello, [], classy_lib:rpc_timeout()) of
    #{ site := Remote
     , cluster := Cluster
     , pid := _RemotePid
     , mem_data := MemData
     } ->
      case classy_hook:all(?on_pre_join, [Cluster, Remote, Node, Intent]) of
        ok ->
          do_join_node(Node, Cluster, Remote, MemData, S);
        {error, _} = Err ->
          Err
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
      %% Already in the same cluster with `Node'. Set our membership
      %% status and trigger re-sync (do we need to re-run hooks?):
      classy_membership:cast_sync(Cluster, Local, MemData),
      classy_membership:set_member(Cluster, Local, Local, true),
      classy_membership:flush(Cluster, Local),
      {ok, adjust_run_level(S0)};
    {ok, OldCluster} when OldCluster =/= Cluster ->
      %% Site is currently in a different cluster. Leave it first:
      case leave_cluster(OldCluster, Local, S0, join) of
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

leave_cluster(Cluster, Local, S, Intent) ->
  case handle_kick(Cluster, Local, Local, Intent) of
    ok ->
      {ok, on_leave(S, Intent)};
    Err ->
      Err
  end.

on_leave(S0 = #s{cluster = Cluster, site = Local}, Intent) ->
  S = change_run_level(run_level(?stopped), S0),
  classy_table:delete(?ptab, ?the_cluster),
  classy_hook:foreach(?on_post_kick, [Cluster, Local, Intent]),
  S#s{ cluster = undefined
     }.

join_cluster(Cluster, Local, S = #s{run_level = 0}) ->
  {ok, _} = classy_sup:ensure_membership(Cluster, Local),
  classy_hook:foreach(?on_post_join, [Cluster, Local]),
  set_val(?the_cluster, Cluster),
  {ok, S#s{cluster = Cluster}}.

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


set_val(Key, Val) when is_binary(Val), Key =/= ?the_site orelse Key =/= ?the_cluster ->
  classy_table:write(?ptab, Key, Val).

-spec adjust_run_level(#s{}) -> #s{}.
adjust_run_level(S = #s{cluster = Cluster, site = Site}) ->
  NKnown = length(classy_membership:members(Cluster, Site)),
  NRunning = length(classy:nodes(running)),
  RunLevel = case NKnown >= classy_lib:n_sites() of
               true  ->
                 case NRunning >= classy:quorum(config) of
                   true  -> run_level(?quorum);
                   false -> run_level(?cluster)
                 end;
               false -> run_level(?single)
             end,
  change_run_level(RunLevel, S).

-spec change_run_level(run_level_int(), #s{}) -> #s{}.
change_run_level(Level, #s{run_level = Level} = S) when is_integer(Level) ->
  S;
change_run_level(To, #s{run_level = From} = S) when To >= 0, To =< 3 ->
  Next = if To > From ->
             From + 1;
            To < From ->
             From - 1
         end,
  classy_hook:foreach(?on_change_run_level, [run_level(From), run_level(Next)]),
  change_run_level(To, S#s{run_level = Next}).

-spec run_level(run_level_int()) -> run_level_atom();
               (run_level_atom()) -> run_level_int().
run_level(?stopped) -> 0;
run_level(?single)  -> 1;
run_level(?cluster) -> 2;
run_level(?quorum)  -> 3;
run_level(0) -> ?stopped;
run_level(1) -> ?single;
run_level(2) -> ?cluster;
run_level(3) -> ?quorum.
