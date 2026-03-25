%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_node).

-behavior(gen_server).

%% API:
-export([ start_link/0
        , maybe_init_the_site/1
        , join_node/2
        , kick_site/2
        , the_site/0
        , the_cluster/0

        , at_lower_level/2
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([hello/0]).

-export_type([run_level_atom/0]).

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
-record(call_at_run_level,
        { level :: run_level_atom()
        , function :: fun(() -> _)
        }).

-define(site_info, classy_site_status_tab).

-record(site_info, {isup, node, last_alive_at}).

-type run_level_int() :: 0..3.
-type run_level_atom() :: ?stopped | ?single | ?cluster | ?quorum.

%%================================================================================
%% API functions
%%================================================================================

%% @doc Initialize the local site.
%%
%% Values that are not persistently stored are set to the given values.
%% Any `undefined' argument is replaced with a sufficiently unique random string.
-spec maybe_init_the_site(classy:site() | undefined) -> ok.
maybe_init_the_site(MaybeSite) ->
  {_, Site} = ensure_value(?the_site, ?on_create_site, [], MaybeSite),
  _ = ensure_value(?the_cluster, ?on_create_cluster, [Site], undefined),
  ok.

%% @private
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Return ID of the cluster that the node currently belongs to.
-spec the_cluster() -> {ok, classy:cluster_id()} | undefined.
the_cluster() ->
  case classy_table:lookup(?ptab, ?the_cluster) of
    [V] -> {ok, V};
    []   -> undefined
  end.

%% @doc Return ID of the local site.
-spec the_site() -> {ok, classy:site()} | undefined.
the_site() ->
  case classy_table:lookup(?ptab, ?the_site) of
    [V] -> {ok, V};
    []  -> undefined
  end.

%% @doc Join to the cluster that `Node' belongs to.
%%
%% This function performs all necessary checks before making any changes.
-spec join_node(node(), _Intent) -> ok | {error, _}.
join_node(Node, Intent) ->
  gen_server:call(
    ?SERVER,
    #call_join{node = Node, intent = Intent},
    infinity).

%% @doc Kick a site from the cluster.
%%
%% This function performs all necessary checks before making any changes.
%% It can be used with the local site as well.
-spec kick_site(classy:site(), _Intent) -> ok | {error, _}.
kick_site(Site, Intent) ->
  gen_server:call(
    ?SERVER,
    #call_kick{site = Site, intent = Intent},
    infinity).

%% @doc Lower the run level to the given value and run the specified function.
-spec at_lower_level(run_level_atom(), fun(() -> Ret)) ->
        {ok, Ret} |
        {error | exit | throw, _Reason, _Stacktrace}.
at_lower_level(RunLevel, Fun) ->
  gen_server:call(
    ?SERVER,
    #call_at_run_level{level = RunLevel, function = Fun},
    infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { cluster :: classy:cluster_id() | undefined
        , site :: classy:site()
        , run_level = 0 :: run_level_int()
        }).

%% @private
init(_) ->
  process_flag(trap_exit, true),
  net_kernel:monitor_nodes(
    true,
    #{ node_type => visible
     , nodedown_reason => true
     }),
  ok = classy_table:open(?ptab, #{}),
  ok = classy_table:open(?site_info, #{}),
  classy:on_membership_change(fun on_membership_change/4, -100),
  classy_hook:foreach(?on_node_init, []),
  case init_cluster() of
    {ok, _} = Ok ->
      Ok;
    {error, Reason} ->
      {stop, Reason, undefined}
  end.

%% @private
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
handle_call(#call_at_run_level{level = RequestedRunLevel, function = Fun}, _From, S0) ->
  RunLevel = min(S0#s.run_level, run_level(RequestedRunLevel)),
  S = change_run_level(RunLevel, S0),
  Ret = try
          {ok, Fun()}
        catch
          EC:Err:Stack ->
            {EC, Err, Stack}
        end,
  {reply, Ret, adjust_run_level(S)};
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

%% @private
handle_cast(#cast_membership_change{} = Cast, S) ->
  handle_membership_change_event(Cast, S);
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

%% @private
handle_info({NodeUpOrDown, _Node, _}, S) when NodeUpOrDown =:= nodeup; NodeUpOrDown =:= nodedown ->
  update_sites_status(S),
  {noreply, adjust_run_level(S)};
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

%% @private
terminate(Reason, S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  classy_table:stop(?ptab, 1_000),
  classy_table:stop(?site_info, 1_000),
  case S of
    #s{} -> change_run_level(run_level(?stopped), S);
    _    -> ok
  end.

%%================================================================================
%% Internal exports
%%================================================================================

%% @doc Called by remote node during `join'.
%% Returns information about the local site, used for bootstrapping the remote.
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
  S0 = #s{cluster = ThisCluster, site = ThisSite}
 ) ->
  ?tp(debug, membership_change,
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
      ?tp(warning, classy_kicked_remotely,
          #{ cluster => Cluster
           }),
      case on_leave(S0, kicked) of
        {ok, S}      -> {noreply, S};
        {error, Err} -> {stop, Err, undefined}
      end;
     Cluster =:= ThisCluster ->
      update_sites_status(S0),
      {noreply, adjust_run_level(S0)};
     true ->
      {noreply, S0}
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
      Intent = join,
      case handle_kick(OldCluster, Local, Local, Intent) of
        ok ->
          {ok, S} = on_leave(S0, Intent),
          do_join_node(Node, Cluster, Remote, MemData, S);
        Err ->
          Err
      end;
    undefined ->
      %% Site is not in any cluster:
      {ok, S} = join_cluster(Cluster, Local, S0),
      do_join_node(Node, Cluster, Remote, MemData, S)
  end.

on_leave(S0 = #s{cluster = Cluster, site = Local}, Intent) ->
  S = change_run_level(run_level(?stopped), S0),
  classy_table:delete(?ptab, ?the_cluster),
  classy_hook:foreach(?on_post_kick, [Cluster, Local, Intent]),
  classy_table:clear(?site_info),
  case Intent of
    join ->
      {ok, S#s{cluster = undefined}};
    _ ->
      init_cluster()
  end.

join_cluster(Cluster, Local, S = #s{run_level = 0}) ->
  {ok, _} = classy_sup:ensure_membership(Cluster, Local),
  classy_hook:foreach(?on_post_join, [Cluster, Local]),
  set_val(?the_cluster, Cluster),
  {ok, S#s{cluster = Cluster}}.

update_sites_status(S = #s{cluster = Cluster, site = Site}) ->
  Nodes = [node() | nodes()],
  Members = classy_membership:members(Cluster, Site),
  NodesOfSite = classy_membership:node_of_site(Cluster, Site),


init_cluster() ->
  maybe
    {ok, Cluster} ?= the_cluster(),
    {ok, Site} ?= the_site(),
    logger:update_process_metadata(
      #{ local => Site
       }),
    {ok, _} = classy_sup:ensure_membership(Cluster, Site),
    S = adjust_run_level(
          #s{ cluster = Cluster
            , site = Site
            }),
    ?tp(debug, classy_init_clustering, #{local => Site, cluster => Cluster}),
    update_sites_status(S),
    {ok, S}
  else
    _ ->
      {error, default_site_not_initialized}
  end.

-spec ensure_value(?the_cluster | ?the_site, ?on_create_cluster | ?on_create_site, list(), binary() | undefined) ->
        {boolean(), binary()}.
ensure_value(Key, OnCreateHook, HookArgs, Default) ->
  case classy_table:lookup(?ptab, Key) of
    [Bin] when is_binary(Bin) ->
      {false, Bin};
    [] ->
      case Default of
        undefined ->
          Val = base64:encode(crypto:strong_rand_bytes(32));
        Val when is_binary(Val) ->
          ok
      end,
      classy_hook:foreach(OnCreateHook, [Val | HookArgs]),
      set_val(Key, Val),
      {true, Val}
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
