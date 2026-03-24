%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("proper/include/proper.hrl").

-define(ON(SITE, BODY), classy_test_site:call(SITE, fun() -> BODY end)).

-define(assertSameSet(EXP, GOT), ?assertEqual(lists:sort(EXP), lists:sort(GOT))).

%%================================================================================
%% Tests
%%================================================================================

t_010_cluster(_Conf) ->
  ?check_trace(
     #{timetrap => 15_000},
     begin
       %% Create site and ensure that this operation is idempotent:
       ?assertMatch(ok, classy_test_cluster:ensure_site(<<"foo">>, #{})),
       ?assertMatch(ok, classy_test_cluster:ensure_site(<<"foo">>, #{})),
       %% Check that error message is legible when calling a stopped site:
       ?assertError(
          {site_is_not_running, <<"foo">>},
          classy_test_site:call(<<"foo">>,
                                fun() ->
                                    ok
                                end)),
       %% Start site:
       ?assertMatch(ok, classy_test_site:start(<<"foo">>)),
       ?assertMatch({error, already_started}, classy_test_site:start(<<"foo">>)),
       %% Test calls and log forwarding:
       ?assertMatch('foo@127.0.0.1', classy_test_site:call(<<"foo">>, erlang, node, [])),
       ?assertMatch(
          ok,
          classy_test_site:call(<<"foo">>,
                                fun() ->
                                    ?tp(test_msg_from_foo, #{})
                                end)),
       ?block_until(#{?snk_kind := test_msg_from_foo}),
       %% Test stopping idempotency:
       ?assertMatch(ok, classy_test_site:stop(<<"foo">>)),
       ?assertMatch(ok, classy_test_site:stop(<<"foo">>))
     end,
     []).

%% This testcase verifies happy case of joining one node to another:
t_020_join(Conf) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  ?check_trace(
     #{timetrap => 10_000},
     begin
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       #{ site := S1
        , cluster := Cluster1
        } = ?ON(S1, classy_node:hello()),
       #{ site := S2
        , cluster := Cluster2
        } = ?ON(S2, classy_node:hello()),
       RuntimeData = #{ nodes => [N1, N2]
                      , sites => [S1, S2]
                      , clusters => [Cluster1, Cluster2]
                      },
       %% Verify status of the nodes in the singleton mode. Both
       %% should belong to the cluster consisting of a single site,
       %% cluster ID should be equal to the site id:
       ?assertEqual(
          {ok, Cluster1},
          ?ON(S1, classy_node:the_cluster())),
       ?assertEqual(
          [S1],
          ?ON(S1, classy:sites())),
       ?assertEqual(
          {ok, Cluster2},
          ?ON(S2, classy_node:the_cluster())),
       ?assertEqual(
          [S2],
          ?ON(S2, classy:sites())),
       %% Join the nodes:
       ?tp(notice, test_join_n2, RuntimeData),
       ?assertMatch(
          ok,
          ?ON(S2, classy:join_node(N1, join))),
       wait_site_joined([S1, S2], Cluster1, S2),
       %% Verify state after join:
       ?assertEqual(
          {ok, Cluster1},
          ?ON(S1, classy_node:the_cluster())),
       ?assertEqual(
          {ok, Cluster1},
          ?ON(S2, classy_node:the_cluster())),
       ?assertSameSet(
          [S1, S2],
          ?ON(S1, classy:sites())),
       ?assertSameSet(
          [S1, S2],
          ?ON(S2, classy:sites())),
       RuntimeData
     end,
     [ fun initialization_hooks/2
     , {"join hooks",
        fun(Trace) ->
            ?assert(
               ?strict_causality(
                  #{?snk_kind := classy_pre_join_node, cluster := C},
                  #{?snk_kind := classy_joined_cluster, cluster := C},
                  Trace))
        end}
     ]).

%% This testcase verifies happy case of kicking node from the cluster:
t_030_kick(Conf) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  ?check_trace(
     begin
       %% Prepare the system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       #{ site := S1
        , cluster := Cluster1
        } = ?ON(S1, classy_node:hello()),
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertMatch(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster1, S2),
       wait_site_joined(Sites, Cluster1, S3),
       %% Verify state:
       [?assertSameSet(
           Sites,
           ?ON(I, classy:sites()))
        || I <- Sites],
       %% Kick N1 from the cluster from N3:
       {ok, SubRef} = snabbkaffe:subscribe(?match_event(#{?snk_kind := classy_init_clustering})),
       ?assertMatch(ok, ?ON(S3, classy:kick_node(N1, force))),
       %% Wait for completion of the operation:
       {ok, _} = snabbkaffe:receive_events(SubRef),
       wait_site_kicked(Sites, Cluster1, S1),
       %% Verify state:
       [?assertSameSet(
           [S2, S3],
           ?ON(I, classy:sites()))
        || I <- [S2, S3]],
       ?assertEqual(
          [S1],
          ?ON(S1, classy:sites())),
       #{ nodes => [N1, N2, N3]
        , sites => Sites
        , clusters => [Cluster1]
        }
     end,
     [
     ]).

%% Verify that node can be kicked from the cluster while down:
t_040_kick_in_absentia(Conf) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  ?check_trace(
     begin
       %% Prepare the system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       #{ site := S1
        , cluster := Cluster1
        } = ?ON(S1, classy_node:hello()),
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertMatch(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster1, S2),
       wait_site_joined(Sites, Cluster1, S3),
       %% Stop S1:
       classy_test_site:stop(S1),
       %% Kick S1 from the cluster from S:
       ?assertMatch(ok, ?ON(S3, classy:kick_node(N1, kick))),
       wait_site_kicked([S2, S3], Cluster1, S1),
       %% Verify state:
       [?assertSameSet(
           [S2, S3],
           ?ON(I, classy:sites()))
        || I <- [S2, S3]],
       %% Bring S1 back up.
       %%   Upon realization that it got kicked, it should create a fresh cluster:
       {ok, SubRef} = snabbkaffe:subscribe(?match_event(#{ ?snk_kind := classy_init_clustering
                                                         , site := S1
                                                         , cluster := C
                                                         } when C =/= Cluster1)),
       ok = classy_test_site:start(S1),
       %% It should process the information about getting kicked:
       wait_site_kicked([S1], Cluster1, S1),
       {ok, _} = snabbkaffe:receive_events(SubRef),
       %% It should not rejoin the old cluster:
       [?assertSameSet(
           [S2, S3],
           ?ON(I, classy:sites()))
        || I <- [S2, S3]],
       %% It should forma a new singleton cluster instead:
       ?assertEqual(
          [S1],
          ?ON(S1, classy:sites())),
       #{ nodes => [N1, N2, N3]
        , sites => Sites
        , clusters => [Cluster1]
        }
     end,
     [ {"kicked_remotely_event",
        fun(#{nodes := [N1 | _]}, Trace) ->
            ?assertMatch(
               [_],
               [I || I = #{ ?snk_kind := classy_kicked_remotely
                          , ?snk_meta := #{node := N1}
                          } <- Trace])
        end}
     ]).

%% Verify that join and kick can be forbidden via hooks:
t_050_pre_checks(Conf) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  Sites = [S1, S2],
  ?check_trace(
     begin
       %% Prepare the system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       #{cluster := Cluster2} = ?ON(S2, classy_node:hello()),
       %% Inject hooks:
       ?ON(S1, classy:pre_join(
                 fun(_Cluster, _Remote, _Node, Intent) ->
                     case Intent of
                       force -> ok;
                       _ -> {error, forbidden}
                     end
                 end,
                 0)),
       ?ON(S2, classy:pre_kick(
                 fun(_Cluster, _Remote, Intent) ->
                     case Intent of
                       force -> ok;
                       _ -> {error, forbidden}
                     end
                 end,
                 0)),
       %% Join is forbidden:
       ?assertEqual(
          {error, forbidden},
          ?ON(S1, classy:join_node(N2, join))),
       %% Force join:
       ?assertEqual(
          ok,
          ?ON(S1, classy:join_node(N2, force))),
       wait_site_joined(Sites, Cluster2, S1),
       %% Kick is forbidden:
       ?assertEqual(
          {error, forbidden},
          ?ON(S2, classy:kick_node(N1, kick))),
       %% Force kick:
       ?assertEqual(
          ok,
          ?ON(S2, classy:kick_node(N1, force)))
     end,
     []).

t_999_fuzz(_Config) ->
  %% NOTE: we set timeout at the lowest level to capture the trace
  %% and have a nicer error message.
  %%
  %% By default the number of tests and max_size are set to low
  %% values to avoid blowing up CI. Hence it's recommended to
  %% increase the max_size and numtests when doing local
  %% development using "apps/emqx/test/sessds.cfg"
  NTests = ct:get_config({fuzzer, n_tests}, 10),
  MaxSize = ct:get_config({fuzzer, max_size}, 100),
  NCommandsFactor = ct:get_config({fuzzer, command_multiplier}, 1),
  ?run_prop(
     #{ proper =>
          #{ timeout => 3_000_000
           , numtests => NTests
           , max_size => MaxSize
           , start_size => MaxSize
           , max_shrinks => 0
           }
      },
      ?forall_trace(
         Cmds,
         proper_statem:more_commands(
           NCommandsFactor,
           proper_statem:commands(
             classy_test_fuzzer,
             classy_fuzzer:initial_state(#{ module => ?MODULE
                                          , sites => [ {<<"foo">>, #{}}
                                                     , {<<"bar">>, #{}}
                                                     ]
                                          }))),
         #{timetrap => 5_000 * length(Cmds) + 30_000},
         try
           %% Print information about the run:
           io:format(user, "*** Commands:~n~s~n", [classy_test_fuzzer:format_cmds(Cmds)]),
           %% Initialize the system:
           classy_test_cluster:start_link(
             #{ fixtures => classy_test_fixture:defaults(?FUNCTION_NAME)
              }),
           %% Run test:
           {_History, State, Result} = proper_statem:run_commands(classy_test_fuzzer, Cmds),
           ct:log(info, "*** Model state:~n  ~p~n", [State]),
           ct:log("*** Result:~n  ~p~n", [Result]),
           Result =:= ok orelse error({invalid_result, Result})
         after
           ok = classy_test_cluster:stop(normal)
         end,
         [
         ])),
  snabbkaffe:stop().

%%================================================================================
%% Internal functions
%%================================================================================

init_per_suite(Cfg) ->
  classy_ct:create_table(),
  Cfg.

end_per_suite(Cfg) ->
  Cfg.

init_per_testcase(t_fuzz, Cfg) ->
  Cfg;
init_per_testcase(TC, Cfg) ->
  Fixtures = [ {classy_test_snabbkaffe, #{}}
             ],
  {ok, _} = classy_test_cluster:start_link(
              #{ fixtures => classy_test_fixture:defaults(TC) ++ Fixtures
               }),
  Cfg.

create_start_site(Site, CustomConf) ->
  Fixture = {classy_test_app,
             #{ app => classy
              , env => #{setup_hooks => {?MODULE, setup_hooks, [Site]}}
              }},
  Conf = CustomConf#{fixtures => [Fixture]},
  ?assertMatch(ok, classy_test_cluster:ensure_site(Site, Conf)),
  ?assertMatch(ok, classy_test_site:start(Site)),
  classy_test_site:which_node(Site).

end_per_testcase(_TC, Cfg) ->
  ct:pal("EXIT ~p", [Cfg]),
  classy_test_cluster:stop(normal),
  snabbkaffe:stop().

all() ->
  classy_ct:all(?MODULE).

wait_site_joined(WaitOnSites, Cluster, Site) ->
  lists:foreach(
    fun(S) ->
        Node = classy_test_site:which_node(S),
        ?block_until(
           #{ ?snk_kind := classy_member_join
            , cluster := Cluster
            , site := Site
            , ?snk_meta := #{node := Node}
            })
    end,
    WaitOnSites).

wait_site_kicked(WaitOnSites, Cluster, Site) ->
  lists:foreach(
    fun(S) ->
        Node = classy_test_site:which_node(S),
        ?block_until(
           #{ ?snk_kind := classy_member_leave
            , cluster := Cluster
            , site := Site
            , ?snk_meta := #{node := Node}
            })
    end,
    WaitOnSites).

initialization_hooks(RuntimeData, Trace) ->
  #{ nodes := Nodes
   , sites := Sites
   , clusters := Clusters
   } = RuntimeData,
  ?assertSameSet(
     Nodes,
     ?projection(node, ?of_kind(classy_on_node_init, Trace))),
  ?assertSameSet(
     Sites,
     ?projection(site, ?of_kind(classy_create_new_site, Trace))),
  ?assertSameSet(
     Clusters,
     ?projection(cluster, ?of_kind(classy_create_new_cluster, Trace))).

setup_hooks(Site) ->
  classy:on_node_init(
    fun() ->
        classy_node:maybe_init_the_site(Site)
    end,
    0).
