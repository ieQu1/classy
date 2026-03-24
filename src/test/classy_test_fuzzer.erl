%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A helper module for implementing property based tests on
%% classy clusters.
-module(classy_test_fuzzer).

%% API:
-export([ format_cmds/1
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
        , setup_hooks/2
        , join_node/3
        , kick_site/3
        ]).

-export_type([test_conf/0]).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").

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
         }.

-type s() ::
        #{ module := module()
         , sites := site_state()
         , quorum := pos_integer()
         , n_sites := pos_integer()
         , _ => _
         }.

%%================================================================================
%% Internal exports
%%================================================================================

-spec init_cluster(s()) -> ok.
init_cluster(#{sites := Sites, quorum := Quorum, n_sites := NSites}) ->
  lists:foreach(
    fun({Site, Conf0}) ->
        Fixtures = maps:get(fixtures, Conf0, []),
        ClassyFixture = {classy_test_app,
                         #{ app => classy
                          , env => #{ setup_hooks => {?MODULE, setup_hooks, [Site, Site]}
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

setup_hooks(Cluster, Site) ->
  classy:on_node_init(
    fun() ->
        classy_node:maybe_init_the_site(Cluster, Site)
    end,
    0),
  classy:post_kick(
    fun(_OldCluster, Local, Intent) ->
        Intent =/= join andalso
          classy_node:maybe_init_the_site(Local, Local)
    end,
    -100).

join_node(Origin, Target, Intent) ->
  TargetNode = classy_test_site:which_node(Target),
  do_join_node(Origin, TargetNode, Intent, 3).

do_join_node(Origin, TargetNode, Intent, Retries) ->
  Result = classy_test_site:call(
             Origin,
             fun() ->
                 classy:join_node(TargetNode, Intent)
             end),
  case Result of
    ok ->
      ok;
    {error, not_in_cluster} when Retries > 0 ->
      %% Note: we use retries since there's a temporary state
      %% immediately after kick, when node left the old cluster, but
      %% hasn't joined its own "singleton" cluster yet.
      do_join_node(Origin, TargetNode, Intent, Retries - 1);
    Other ->
      Other
  end.

kick_site(Origin, Target, Intent) ->
  classy_test_site:call(
    Origin,
    fun() ->
        classy:kick_site(Target, Intent)
    end).

%%================================================================================
%% Utility functions
%%================================================================================

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
    [ {1, {call, ?MODULE, kick_site, [Site, oneof(OtherMembers), kick]}} || length(OtherMembers) > 0] ++
    [ {1, {call, classy_test_site, stop, [Site]}}
    , {1, {call, ?MODULE, join_node, [Site, oneof(running_sites(S)), join]}}
    ]).

stopped_site_command_(Site, S) ->
  frequency(
    [ {1, {call, classy_test_site, start, [Site]}}
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
command(S = #{module := CBM, sites := Sites}) ->
  SiteCmds = [{1, site_command_(Site, S)} || Site <- maps:keys(Sites)],
  frequency(SiteCmds).

-spec initial_state(test_conf()) -> s().
initial_state(Conf) ->
  {init, Conf}.

%% Initial connection:
next_state(_, _Ret, {call, ?MODULE, init_cluster, [TestConf]}) ->
  #{sites := Sites0} = TestConf,
  Sites = maps:from_list(
            [ { Site
              , #{ cluster => Site
                 , running => false
                 , conf    => Conf
                 }
              }
            || {Site, Conf} <- Sites0]),
  TestConf#{ sites := Sites
           };
next_state(S, _Ret, {call, classy_test_site, start, [Site]}) ->
  update_site(
    Site,
    fun(SState) -> SState#{running := true} end,
    S);
next_state(S, _Ret, {call, classy_test_site, stop, [Site]}) ->
  update_site(
    Site,
    fun(SState) -> SState#{running := false} end,
    S);
next_state(S = #{sites := Sites}, _Ret, {call, ?MODULE, join_node, [Origin, Target, _Intent]}) ->
  #{Target := #{cluster := Cluster}} = Sites,
  update_site(
    Origin,
    fun(SState) -> SState#{cluster := Cluster} end,
    S);
next_state(S, _Ret, {call, ?MODULE, kick_site, [_Origin, Target, _Intent]}) ->
  update_site(
    Target,
    fun(SState) -> SState#{cluster := Target} end,
    S).

precondition(_, _) ->
    true.

postcondition(PrevState, Call, Result) ->
  CurrentState = next_state(PrevState, Result, Call),
  case Call of
   {call, ?MODULE, join_node, _} ->
      ?assertMatch(ok, Result);
    _ ->
      true
  end,
  check_invariants(CurrentState).

%%================================================================================
%% Internal functions
%%================================================================================

maybe_generate(Key, Conf, Generator) ->
  case Conf of
    #{Key := Val} -> exactly(Val);
    #{}           -> Generator
  end.

is_running(Site, #{sites := Sites}) ->
  #{Site := #{running := Ret}} = Sites,
  Ret.

check_invariants(_CurrentState) ->
  true.

update_site(Site, Fun, S = #{sites := Sites}) ->
  S#{sites := maps:update_with(Site, Fun, Sites)}.

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
