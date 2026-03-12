%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ON(NODE, BODY), classy_ct:rpc(NODE, erlang, apply, [fun() -> BODY end, []])).

-define(assertSameSet(EXP, GOT), ?assertEqual(lists:sort(EXP), lists:sort(GOT))).

%%================================================================================
%% Tests
%%================================================================================

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
       ?assertMatch(ok, classy_ct:rpc(N2, classy, join_node, [maps:get(node, N1)])),
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
       ?assertMatch(ok, ?ON(N2, classy:join_node(Node1))),
       ?assertMatch(ok, ?ON(N3, classy:join_node(Node1))),
       wait_site_joined(Nodes, Cluster1, Site2),
       wait_site_joined(Nodes, Cluster1, Site3),
       %% Verify state:
       [?assertSameSet(
           [Site1, Site2, Site3],
           ?ON(Node, classy:sites()))
        || Node <- Nodes],
       %% Kick N1 from the cluster from N3:
       ?assertMatch(ok, ?ON(N3, classy:kick_node(Node1))),
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
  [C1 | _]= Cluster = classy_ct:cluster([#{}, #{}, #{}], []),
  ?check_trace(
     try
       %% Prepare the system:
       Nodes = [N1 = #{node := Node1}, N2, N3] = classy_ct:start_cluster(classy, Cluster),
       #{ site := Site1
        , cluster := Cluster1
        } = classy_ct:rpc(N1, classy_node, hello, []),
       #{site := Site2} = ?ON(N2, classy_node:hello()),
       #{site := Site3} = ?ON(N3, classy_node:hello()),
       ?assertMatch(ok, ?ON(N2, classy:join_node(Node1))),
       ?assertMatch(ok, ?ON(N3, classy:join_node(Node1))),
       wait_site_joined(Nodes, Cluster1, Site2),
       wait_site_joined(Nodes, Cluster1, Site3),
       %% Stop N1:
       classy_ct:stop_peer(N1),
       %% Kick N1 from the cluster from N3:
       ?assertMatch(ok, ?ON(N3, classy:kick_node(Node1))),
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
     [
     ]).

%%================================================================================
%% Internal functions
%%================================================================================

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

end_per_testcase(_, _) ->
  snabbkaffe:stop().
