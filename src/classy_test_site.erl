%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_test_site).

-behavior(gen_server).

%% API:
-export([ is_running/1
        , which_node/1

        , start/1
        , start/2
        , stop/1

        , call/2
        , call/4
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([ start_link/4
        ]).

-export_type([conf/0]).

-include_lib("common_test/include/ct.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(site(SITE), {n, l, {?MODULE, SITE}}).
-define(node_name(NODE), {n, l, {classy_test_node, NODE}}).
-define(via(SITE), {via, gproc, ?site(SITE)}).
-define(call_via(SITE), {?MODULE, SITE}).

-type conf() ::
            #{ peer => map()
             , fixtures => [classy_test_fixture:t()]
             , _ => _
             }.

-record(call_is_running, {}).
-record(call_start, {name :: node()}).
-record(call_stop, {}).

%%================================================================================
%% API functions
%%================================================================================

%% @private
-spec start_link(conf(), classy_test_fixture:state(), classy:site(), conf()) -> {ok, pid()}.
start_link(CommonSpec, FixtureState, Site, Spec) ->
  gen_server:start_link(?via(Site), ?MODULE, [CommonSpec, FixtureState, Site, Spec], []).

%% @doc Is site running?
-spec is_running(classy:site()) -> boolean().
is_running(Site) ->
  gen_server:call(?via(Site), #call_is_running{}).

%% @doc Return current node name of the site.
%% Throws an error if site is not running.
-spec which_node(classy:site()) -> node().
which_node(Site) ->
  case call_method(Site) of
    {erpc, Node} ->
      Node
  end.

%% @doc Start the site if stopped.
%%
%% Can return `{error, already_running}'.
-spec start(classy:site()) -> ok | {error, _}.
start(Site) ->
  start(Site, binary_to_atom(Site)).


%% @doc Start the site if stopped, using `NodeName' as a prefix for the node.
%% Resulting node will be named `NodeName@Host'.
%%
%% Can return `{error, already_running}'.
-spec start(classy:site(), atom()) -> ok | {error, _}.
start(Site, NodeName) ->
  gen_server:call(?via(Site), #call_start{name = NodeName}, infinity).

%% @doc Stop the site's node.
%%
%% Note: this function doesn't destroy the site: it can be restarted later.
-spec stop(classy:site()) -> ok.
stop(Site) ->
  gen_server:call(?via(Site), #call_stop{}, infinity).

%% @doc Execute MFA on the site.
%% Site must be running.
-spec call(classy:site(), module(), atom(), list()) -> _.
call(Site, Module, Function, Args) ->
  case call_method(Site) of
    {erpc, Node} ->
      erpc:call(Node, Module, Function, Args)
  end.

%% @doc Execute `Fun' on the site.
%% Site must be running.
-spec call(classy:site(), fun(() -> Ret)) -> Ret.
call(Site, Fun) ->
  case call_method(Site) of
    {erpc, Node} ->
      erpc:call(Node, Fun)
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { site               :: classy:site()
        , pid                :: pid() | undefined
        , name               :: atom()
        , node               :: node() | undefined
        , spec               :: conf()
        , fixture_state      :: classy_test_fixture:state()
        , node_fixture_state :: map() | undefined
        , my_path            :: string()
        }).

%% @private
init([CommonSpec, FixtureState0, Site, CustomSiteSpec]) ->
  process_flag(trap_exit, true),
  MyPath = filename:dirname(code:which(?MODULE)),
  DefaultCommonSpec =
    #{ peer =>
         #{ longnames => true
          , peer_down => stop
          , host => "127.0.0.1"
          , shutdown => 4_000
          , args => ["+S", "1:1"]
          }
     },
  DefaultSiteSpec =
    #{ peer => #{name => binary_to_atom(Site)}
     },
  SiteSpec = lists:foldr(
               fun classy_test_cluster:merge_conf/2,
               #{},
               [ DefaultCommonSpec
               , CommonSpec
               , DefaultSiteSpec
               , CustomSiteSpec
               ]),
  #{fixtures := Fixtures} = SiteSpec,
  case classy_test_fixture:init_per_site(Fixtures, Site, FixtureState0) of
    {ok, FixtureState} ->
      {ok, #s{ site          = Site
             , spec          = SiteSpec
             , fixture_state = FixtureState
             , my_path       = MyPath
             }};
    {error, Reason} ->
      {stop, Reason}
  end.

%% @private
handle_call(#call_start{name = Name}, _From, S0 = #s{pid = Pid}) ->
  case Pid of
    undefined ->
      case do_start(Name, S0) of
        {ok, S} ->
          {reply, ok, S};
        {error, _} = Err ->
          {reply, Err, S0}
      end;
    _ when is_pid(Pid) ->
      {reply, {error, already_started}, S0}
  end;
handle_call(#call_stop{}, _From, S0 = #s{pid = Pid}) ->
  case Pid of
    undefined ->
      {reply, ok, S0};
    _ when is_pid(Pid) ->
      {ok, S} = do_stop(S0),
      {reply, ok, S}
  end;
handle_call(#call_is_running{}, _From, S = #s{pid = Pid}) ->
  Reply = is_pid(Pid) andalso is_process_alive(Pid),
  {reply, Reply, S};
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

%% @private
handle_cast(_Cast, S) ->
  {noreply, S}.

%% @private
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(_Info, S) ->
  {noreply, S}.

%% @private
terminate(Reason, S0 = #s{site = Site, spec = Spec, fixture_state = FS}) ->
  _ = do_stop(S0),
  Success = classy_test_fixture:exit_reason_to_success(Reason),
  #{fixtures := Fixtures} = Spec,
  classy_test_fixture:cleanup_per_site(Fixtures, Site, Success, FS);
terminate(_Reason, _) ->
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

do_start(Name, S0) ->
  #s{ site = Site
    , spec = #{ fixtures := Fixtures
              , peer     := Peer
              }
    , fixture_state = FS
    , my_path = MyPath
    } = S0,
  #{ args := Args0
   } = Peer,
  Args = [ "-pa", MyPath
         , "-setcookie", atom_to_list(erlang:get_cookie())
         ],
  case gproc:register_name(?node_name(Name), self()) of
    yes ->
      StartArgs = Peer#{ name => Name
                       , args => Args0 ++ Args
                       },
      logger:info("Starting site ~s ~p", [Site, StartArgs]),
      {ok, Pid, Node} = peer:start(StartArgs),
      S = S0#s{ name = Name
              , pid = Pid
              , node = Node
              },
      persistent_term:put(?call_via(Site), {erpc, Node}),
      case classy_test_fixture:init_per_node(Fixtures, Site, Node, FS) of
        {ok, NFS} ->
          {ok, S#s{node_fixture_state = NFS}};
        {error, _} = Err ->
          {ok, _} = do_stop(S),
          Err
      end;
    no ->
      {error, {node_name_conflict, Name}}
  end.

do_stop(S = #s{pid = undefined}) ->
  {ok, S};
do_stop(S) ->
  #s{ spec = #{fixtures := Fixtures}
    , site = Site
    , name = Name
    , pid = Pid
    , node = Node
    , node_fixture_state = NFS
    } = S,
  is_map(NFS) andalso
    classy_test_fixture:cleanup_per_node(Fixtures, Site, Node, NFS),
  persistent_term:erase(?call_via(Site)),
  peer:stop(Pid),
  catch gproc:unregister_name(?node_name(Name)),
  {ok, S#s{ pid = undefined
          , node = undefined
          , name = undefined
          , node_fixture_state = undefined
          }}.

call_method(Site) ->
  case persistent_term:get(?call_via(Site), undefined) of
    undefined ->
      error({site_is_not_running, Site});
    Other ->
      Other
  end.
