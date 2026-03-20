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

t_cluster(_Conf) ->
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
t_join(Conf) ->
  Cluster = classy_ct:cluster([#{}, #{}], []),
  ?check_trace(
     #{timetrap => 10_000},
     try
       Nodes = [N1, N2] = classy_ct:start_cluster(classy, Cluster),
       #{ site := Site1
        , cluster := Cluster1
        } = ?ON(N1, classy_node:hello()),
       #{ site := Site2
        , cluster := Cluster2
        } = ?ON(N2, classy_node:hello()),
       RuntimeData = #{ nodes => Nodes
                      , sites => [Site1, Site2]
                      , clusters => [Cluster1, Cluster2]
                      },
       %% Verify status of the nodes in the singleton mode. Both
       %% should belong to the cluster consisting of a single site,
       %% cluster ID should be equal to the site id:
       ?assertEqual(
          {ok, Cluster1},
          ?ON(N1, classy_node:the_cluster())),
       ?assertEqual(
          [Site1],
          ?ON(N1, classy:sites())),
       ?assertEqual(
          {ok, Cluster2},
          ?ON(N2, classy_node:the_cluster())),
       ?assertEqual(
          [Site2],
          ?ON(N2, classy:sites())),
       %% Join the nodes:
       ?tp(notice, test_join_n2, RuntimeData),
       ?assertMatch(
          ok,
          ?ON(N2, classy:join_node(maps:get(node, N1), join))),
       wait_site_joined(Nodes, Cluster1, Site2),
       %% Verify state after join:
       ?assertEqual(
          {ok, Cluster1},
          ?ON(N1, classy_node:the_cluster())),
       ?assertEqual(
          {ok, Cluster1},
          ?ON(N2, classy_node:the_cluster())),
       ?assertSameSet(
          [Site1, Site2],
          ?ON(N1, classy:sites())),
       ?assertSameSet(
          [Site1, Site2],
          ?ON(N2, classy:sites())),
       RuntimeData
     after
       classy_ct:teardown_cluster(Cluster)
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
t_kick(Conf) ->
  Cluster = classy_ct:cluster([#{}, #{}, #{}], []),
  ?check_trace(
     try
       %% Prepare the system:
       Nodes = [N1 = #{node := Node1}, N2, N3] = classy_ct:start_cluster(classy, Cluster),
       #{ site := Site1
        , cluster := Cluster1
        } = ?ON(N1, classy_node:hello()),
       #{site := Site2} = ?ON(N2, classy_node:hello()),
       #{site := Site3} = ?ON(N3, classy_node:hello()),
       ?assertMatch(ok, ?ON(N2, classy:join_node(Node1, join))),
       ?assertMatch(ok, ?ON(N3, classy:join_node(Node1, join))),
       wait_site_joined(Nodes, Cluster1, Site2),
       wait_site_joined(Nodes, Cluster1, Site3),
       %% Verify state:
       [?assertSameSet(
           [Site1, Site2, Site3],
           ?ON(Node, classy:sites()))
        || Node <- Nodes],
       %% Kick N1 from the cluster from N3:
       ?assertMatch(ok, ?ON(N3, classy:kick_node(Node1, force))),
       wait_site_kicked(Nodes, Cluster1, Site1),
       %% Verify state:
       [?assertSameSet(
           [Site2, Site3],
           ?ON(Node, classy:sites()))
        || Node <- [N2, N3]],
       ?assertEqual(
          [],
          ?ON(N1, classy:sites())),
       #{ nodes => Nodes
        , sites => [Site1, Site2, Site3]
        , clusters => [Cluster1]
        }
     after
       classy_ct:teardown_cluster(Cluster)
     end,
     [
     ]).

%% Verify that node can be kicked from the cluster while down:
t_kick_in_absentia(Conf) ->
  [C1 | _] = Cluster = classy_ct:cluster([#{}, #{}, #{}], []),
  ?check_trace(
     try
       %% Prepare the system:
       Nodes = [N1 = #{node := Node1}, N2, N3] = classy_ct:start_cluster(classy, Cluster),
       #{ site := Site1
        , cluster := Cluster1
        } = classy_ct:rpc(N1, classy_node, hello, []),
       #{site := Site2} = ?ON(N2, classy_node:hello()),
       #{site := Site3} = ?ON(N3, classy_node:hello()),
       ?assertMatch(ok, ?ON(N2, classy:join_node(Node1, join))),
       ?assertMatch(ok, ?ON(N3, classy:join_node(Node1, join))),
       wait_site_joined(Nodes, Cluster1, Site2),
       wait_site_joined(Nodes, Cluster1, Site3),
       %% Stop N1:
       classy_ct:stop_peer(N1),
       %% Kick N1 from the cluster from N3:
       ?assertMatch(ok, ?ON(N3, classy:kick_node(Node1, kick))),
       wait_site_kicked([N2, N3], Cluster1, Site1),
       %% Verify state:
       [?assertSameSet(
           [Site2, Site3],
           ?ON(Node, classy:sites()))
        || Node <- [N2, N3]],
       %% Bring N1 back up:
       N1_1 = classy_ct:start_peer(classy, C1),
       %% It should process the information about getting kicked:
       wait_site_kicked([N1_1], Cluster1, Site1),
       ct:sleep(1000),
       %% It should not reappear in the sites list:
       [?assertSameSet(
           [Site2, Site3],
           ?ON(Node, classy:sites()))
        || Node <- [N2, N3]],
       ?assertEqual(
          [],
          ?ON(N1, classy:sites())),
       #{ nodes => Nodes
        , sites => [Site1, Site2, Site3]
        , clusters => [Cluster1]
        }
     after
       classy_ct:teardown_cluster(Cluster)
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
t_pre_checks(Conf) ->
  Cluster = classy_ct:cluster([#{}, #{}], []),
  ?check_trace(
     try
       %% Prepare the system:
       Nodes = [N1 = #{node := Node1}, N2 = #{node := Node2}]
         = classy_ct:start_cluster(classy, Cluster),
       ?ON(N1, classy:pre_join(
                 fun(_Cluster, _Remote, _Node, Intent) ->
                     case Intent of
                       force -> ok;
                       _ -> {error, forbidden}
                     end
                 end,
                 0)),
       ?ON(N2, classy:pre_kick(
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
          ?ON(N1, classy:join_node(Node2, join))),
       %% Force join:
       ?assertEqual(
          ok,
          ?ON(N1, classy:join_node(Node2, force))),
       %% Kick is forbidden:
       ?assertEqual(
          {error, forbidden},
          ?ON(N2, classy:kick_node(Node1, kick))),
       %% Force kick:
       ?assertEqual(
          ok,
          ?ON(N2, classy:kick_node(Node1, force)))
     after
       classy_ct:teardown_cluster(Cluster)
     end,
     []).

t_fuzz(_Config) ->
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
             classy_fuzzer,
             classy_fuzzer:initial_state(#{}))),
         #{timetrap => 5_000 * length(Cmds) + 30_000},
         try
           %% Print information about the run:
           ct:pal("*** Commands:~n~s~n", [classy_fuzzer:format_cmds(Cmds)]),
           %% Initialize the system:
           classy_fuzzer:cleanup(),
           %% Run test:
           {_History, State, Result} = proper_statem:run_commands(classy_fuzzer, Cmds),
           ct:log(info, "*** Model state:~n  ~p~n", [State]),
           ct:log("*** Result:~n  ~p~n", [Result]),
           Result =:= ok orelse error({invalid_result, Result})
         after
           ok = classy_fuzzer:cleanup()
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

init_per_testcase(TC, Cfg) ->
  Fixtures = [ {classy_test_snabbkaffe, #{}}
             ],
  {ok, _} = classy_test_cluster:start_link(
              #{ fixtures => classy_test_fixture:defaults(TC) ++ Fixtures
               }),
  Cfg.

end_per_testcase(_TC, Cfg) ->
  ct:pal("EXIT ~p", [Cfg]),
  classy_test_cluster:stop(normal),
  snabbkaffe:stop().

all() ->
  classy_ct:all(?MODULE).

wait_site_joined(Nodes, Cluster, Site) ->
  lists:foreach(
    fun(#{node := Node}) ->
        ?block_until(
           #{ ?snk_kind := classy_member_join
            , cluster := Cluster
            , site := Site
            , ?snk_meta := #{node := Node}
            })
    end,
    Nodes).

wait_site_kicked(Nodes, Cluster, Site) ->
  lists:foreach(
    fun(#{node := Node}) ->
        ?block_until(
           #{ ?snk_kind := classy_member_leave
            , cluster := Cluster
            , site := Site
            , ?snk_meta := #{node := Node}
            })
    end,
    Nodes).

initialization_hooks(RuntimeData, Trace) ->
  #{ nodes := Nodes
   , sites := Sites
   , clusters := Clusters
   } = RuntimeData,
  ?assertSameSet(
     [maps:get(node, I) || I <- Nodes],
     ?projection(node, ?of_kind(classy_on_node_init, Trace))),
  ?assertSameSet(
     Sites,
     ?projection(site, ?of_kind(classy_create_new_site, Trace))),
  ?assertSameSet(
     Clusters,
     ?projection(cluster, ?of_kind(classy_create_new_cluster, Trace))).
