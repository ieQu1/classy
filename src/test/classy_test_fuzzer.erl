%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A helper module for implementing property based tests on
%% classy clusters.
-module(classy_test_fuzzer).

%% API:
-export([ format_cmds/1
        , cmds/2
        , is_running/2
        , running_sites/1
        , sites_of_cluster/2
        , trace_and_run/1
        , wrap_commands/1
        ]).

%% behavior callbacks:
-export([ command/1
        , initial_state/1
        , next_state/3
        , precondition/2
        , postcondition/3
        ]).

%% internal exports:
-export([ init_cluster/1
        , setup_hooks/1
        , join_node/3
        , kick_site/3
        ]).

-export_type([test_conf/0]).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(SNK_COLLECTOR, true).
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-dialyzer({nowarn_function,
           [ cmds/2
           , command/1
           , running_site_command_/2
           , stopped_site_command_/2
           , enrich_test_conf_/1
           , site_command_/2
           , maybe_generate/3
           ]}).

%%================================================================================
%% Type declarations
%%================================================================================

-type test_conf() ::
        #{ module := module()
         , sites => [{classy:site(), classy_test_site:conf()}]
         , quorum => pos_integer()
         , n_sites => pos_integer()
         , _ => _
         }.

-type site_state() ::
        #{ %% Current cluster ID of the site:
           cluster := classy:cluster_id() | undefined
         , running := boolean()
         , conf := classy_test_site:conf()
         , in_sync := boolean()
         }.

-type s() ::
        #{ module := module()
         , sites := #{classy:site() => site_state()}
         , quorum := pos_integer()
         , n_sites := pos_integer()
           %% Symbolic ID of the cluster (it's not equal to the actual cluster ID, which is random)
         , cluster_id := pos_integer()
         , _ => _
         }.

%%================================================================================
%% Internal exports
%%================================================================================

-spec init_cluster(test_conf()) -> ok.
init_cluster(#{sites := Sites, quorum := Quorum, n_sites := NSites}) ->
  lists:foreach(
    fun({Site, Conf0}) ->
        Fixtures = maps:get(fixtures, Conf0, []),
        ClassyFixture = {classy_test_app,
                         #{ app => classy
                          , env => #{ setup_hooks => {?MODULE, setup_hooks, [Site]}
                                    , quorum => Quorum
                                    , n_sites => NSites
                                    , sync_timeout => 10
                                    }
                          }},
        Conf = Conf0#{fixtures => [ClassyFixture] ++ Fixtures},
        ?assertMatch(
           ok,
           classy_test_cluster:ensure_site(Site, Conf))
    end,
    Sites).

setup_hooks(Site) ->
  classy:on_node_init(
    fun() ->
        classy_node:maybe_init_the_site(Site)
    end,
    0).

join_node(Origin, Target, Intent) ->
  TargetNode = classy_test_site:which_node(Target),
  ?tp(classy_test_fuzzer_join_node,
      #{ site => Origin
       , target => Target
       , target_node => TargetNode
       , intent => Intent
       }),
  do_join_node(Origin, TargetNode, Intent, 1).

do_join_node(Origin, TargetNode, Intent, Retry) ->
  Result = classy_test_site:call(
             Origin,
             fun() ->
                 classy:join_node(TargetNode, Intent)
             end,
             15_000),
  case Result of
    ok ->
      ct:sleep(10),
      ok;
    {error, not_in_cluster} when Retry =< 10 ->
      %% Note: we use retries since there's a temporary state
      %% immediately after kick, when node left the old cluster, but
      %% hasn't joined its own "singleton" cluster yet.
      timer:sleep(Retry * 10),
      do_join_node(Origin, TargetNode, Intent, Retry + 1);
    Other ->
      Other
  end.

kick_site(Origin, Target, Intent) ->
  ?tp(classy_test_fuzzer_kick_site,
      #{ site => Origin
       , target => Target
       , intent => Intent
       }),
  {Ret, _} =
    ?wait_async_action(
       classy_test_site:call(
         Origin,
         fun() ->
             classy:kick_site(Target, Intent)
         end,
         10_000),
       #{ ?snk_kind := classy_member_leave
        , remote    := Target
        , local     := Origin
        },
       5_000),
  Ret.

%%================================================================================
%% Utility functions
%%================================================================================

%% @doc Wrap every command in `trace_and_run' call:
wrap_commands(Cmds) ->
  [case I of
     {set, Var, {call, M, F, A}} ->
       {set, Var, {call, ?MODULE, trace_and_run, [{M, F, A}]}};
     _ ->
       I
   end || I <- Cmds].

trace_and_run(MFA = {M, F, A}) ->
  ?tp_span(debug, classy_test_fuzzer_exec, #{mfa => MFA},
           apply(M, F, A)).

format_cmds(Cmds) ->
  lists:map(
    fun({init, {init, Cfg}}) ->
        io_lib:format(" *** Test configuration: ~p~n", [Cfg]);
       ({call, ?MODULE, init_cluster, [MS]}) ->
        io_lib:format(" *** init(~p)~n", [MS]);
       ({set, _, {call, M, F, Args}}) ->
        ArgsStr = [io_lib:format("~p", [Arg]) || Arg <- Args],
        io_lib:format(" *** ~p:~p(~s)~n", [M, F, lists:join(", ", ArgsStr)]);
       (Other) ->
        io_lib:format(" *** other(~p)~n", [Other])
    end,
    Cmds).

cmds(NCommandsFactor, InitState) ->
  proper_statem:more_commands(
    NCommandsFactor,
    proper_statem:commands(
      ?MODULE,
      initial_state(InitState))).

is_running(Site, #{sites := Sites}) ->
  #{Site := #{running := Running}} = Sites,
  Running.

running_sites(#{sites := Sites}) ->
  maps:fold(
    fun(Site, #{running := Running}, Acc) ->
        case Running of
          true -> [Site | Acc];
          false -> Acc
        end
    end,
    [],
    Sites).

sites_of_cluster(Cluster, #{sites := Sites}) ->
  maps:fold(
    fun(Site, #{cluster := C}, Acc) ->
        if C =:= Cluster -> [Site | Acc];
           true          -> Acc
        end
    end,
    [],
    Sites).

%%================================================================================
%% Proper generators
%%================================================================================

enrich_test_conf_(Conf = #{module := _, sites := Sites}) ->
  ?LET(
     { Quorum
     , NSites
     },
     { maybe_generate(quorum, Conf, range(1, length(Sites)))
     , maybe_generate(n_sites, Conf, range(1, length(Sites)))
     },
     Conf#{ quorum => Quorum
          , n_sites => NSites
          }).

running_site_command_(Site, S = #{sites := Sites}) ->
  #{Site := #{cluster := Cluster}} = Sites,
  OtherMembers = sites_of_cluster(Cluster, S) -- [Site],
  frequency(
    [ {10, {call, ?MODULE, kick_site, [Site, oneof(OtherMembers), kick]}} || length(OtherMembers) > 0] ++
    [ {10, {call, classy_test_site, stop, [Site]}}
    , {10, {call, ?MODULE, join_node, [Site, oneof(running_sites(S)), join]}}
    | optcall(S, running_site_command, [Site, S], [])
    ]).

stopped_site_command_(Site, S) ->
  frequency(
    [ {10, {call, classy_test_site, start, [Site]}}
    | optcall(S, running_site_command, [Site, S], [])
    ]).

site_command_(Site, S) ->
  case is_running(Site, S) of
    true  -> running_site_command_(Site, S);
    false -> stopped_site_command_(Site, S)
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

command({init, Conf0}) ->
  ?LET(Conf,
       enrich_test_conf_(Conf0),
       {call, ?MODULE, init_cluster, [Conf]});
command(S = #{sites := Sites}) ->
  SiteCmds = [{10, site_command_(Site, S)} || Site <- maps:keys(Sites)],
  CustomCmds = optcall(S, general_commands, [S], []),
  frequency(CustomCmds ++ SiteCmds).

-spec initial_state(test_conf()) -> {init, test_conf()}.
initial_state(Conf) ->
  {init, Conf}.

%% Initial connection:
next_state(S, Ret, {call, ?MODULE, trace_and_run, [{M, F, A}]}) ->
  next_state(S, Ret, {call, M, F, A});
next_state(_, _Ret, {call, ?MODULE, init_cluster, [TestConf]}) ->
  #{sites := Sites0} = TestConf,
  {Sites, NextClusterId} =
    lists:mapfoldl(
      fun({Site, Conf}, Acc) ->
          Elem = { Site
                 , #{ cluster => Acc
                    , running => false
                    , conf    => Conf
                    , in_sync => true
                    }
                 },
          {Elem, Acc + 1}
      end,
      0,
      Sites0),
  TestConf#{ sites      := maps:from_list(Sites)
           , cluster_id => NextClusterId
           };
next_state(S, _Ret, {call, classy_test_site, start, [Site]}) ->
  update_site(
    Site,
    fun(SiteS) -> SiteS#{running := true} end,
    S);
next_state(S, _Ret, {call, classy_test_site, stop, [Site]}) ->
  update_site(
    Site,
    fun(SiteS) -> SiteS#{running := false} end,
    S);
next_state(S = #{sites := Sites}, _Ret, {call, ?MODULE, join_node, [Origin, Target, _Intent]}) ->
  #{Target := #{cluster := Cluster}} = Sites,
  update_site(
    Origin,
    fun(SiteS) ->
        %% Joining to a live node syncs the origin:
        SiteS#{cluster := Cluster, in_sync := true}
    end,
    S);
next_state(S, _Ret, {call, ?MODULE, kick_site, [_Origin, Target, _Intent]}) ->
  #{cluster_id := NextClusterId} = S,
  update_site(
    Target,
    fun(SiteS = #{running := Running}) ->
        %% If site is kicked while stopped, we mark it as out-of-sync:
        SiteS#{cluster := NextClusterId, in_sync := Running}
    end,
    S#{cluster_id := NextClusterId + 1});
next_state(S = #{module := Mod}, Ret, Command) ->
  Mod:next_state(S, Ret, Command).

precondition(_, _) ->
    true.

postcondition(PrevState, Call, Result) ->
  CurrentState = next_state(PrevState, Result, Call),
  optcall(CurrentState, postcondition, [CurrentState, Call, Result], true).

%%================================================================================
%% Internal functions
%%================================================================================

maybe_generate(Key, Conf, Generator) ->
  case Conf of
    #{Key := Val} -> exactly(Val);
    #{}           -> Generator
  end.

update_site(Site, Fun, S = #{sites := Sites}) ->
  S#{sites := maps:update_with(Site, Fun, Sites)}.

optcall(#{module := Mod}, Fun, Args, Default) ->
  case erlang:function_exported(Mod, Fun, length(Args)) of
    true  -> apply(Mod, Fun, Args);
    false -> Default
  end.
