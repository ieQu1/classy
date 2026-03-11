%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% Tests
%%================================================================================

%% This testcase verifies normal flow of joining one node to another:
t_join(Conf) ->
  snabbkaffe:fix_ct_logging(),
  Cluster = classy_ct:cluster([#{}, #{}], []),
  ?check_trace(
     #{timetrap => 10_000},
     try
       Nodes = [N1, N2] = classy_ct:start_cluster(classy, Cluster),
       #{ site := Site1
        , cluster := Cluster1
        } = classy_ct:rpc(N1, classy_node, hello, []),
       #{ site := Site2
        , cluster := Cluster2
        } = classy_ct:rpc(N2, classy_node, hello, []),
       RuntimeData = #{ nodes => Nodes
                      , sites => [Site1, Site2]
                      , clusters => [Cluster1, Cluster2]
                      },
       %% Verify status of the nodes in the singleton mode. Both
       %% should belong to the cluster consisting of a single site,
       %% cluster ID should be equal to the site id:
       ?assertEqual(
          {ok, Cluster1},
          classy_ct:rpc(N1, classy_node, the_cluster, [])),
       ?assertEqual(
          [Site1],
          classy_ct:rpc(N1, classy, sites, [])),
       ?assertEqual(
          {ok, Cluster2},
          classy_ct:rpc(N2, classy_node, the_cluster, [])),
       ?assertEqual(
          [Site2],
          classy_ct:rpc(N2, classy, sites, [])),
       %% Join the nodes:
       ?tp(notice, test_join_n2, RuntimeData),
       ?assertMatch(ok, classy_ct:rpc(N2, classy, join_node, [maps:get(node, N1)])),
       %% Wait until both nodes acknowledge the change:
       lists:foreach(
         fun(#{node := Node}) ->
             ?block_until(
                #{ ?snk_kind := classy_member_join
                 , cluster := Cluster1
                 , site := Site2
                 , ?snk_meta := #{node := Node}
                 })
         end,
         Nodes),
       %% Verify state after join:
       ?assertEqual(
          {ok, Cluster1},
          classy_ct:rpc(N1, classy_node, the_cluster, [])),
       ?assertEqual(
          {ok, Cluster1},
          classy_ct:rpc(N2, classy_node, the_cluster, [])),
       ?assertEqual(
          lists:sort([Site1, Site2]),
          lists:sort(classy_ct:rpc(N1, classy, sites, []))),
       ?assertEqual(
          lists:sort([Site1, Site2]),
          lists:sort(classy_ct:rpc(N2, classy, sites, []))),
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

%%================================================================================
%% Internal functions
%%================================================================================

all() ->
  classy_ct:all(?MODULE).

initialization_hooks(RuntimeData, Trace) ->
  #{ nodes := Nodes
   , sites := Sites
   , clusters := Clusters
   } = RuntimeData,
  ?assertEqual(
     lists:sort([maps:get(node, I) || I <- Nodes]),
     lists:sort(
       ?projection(
          node,
          ?of_kind(classy_on_node_init, Trace)))),
  ?assertEqual(
     lists:sort(Sites),
     lists:sort(
       ?projection(
          site,
          ?of_kind(classy_create_new_site, Trace)))),
  ?assertEqual(
     lists:sort(Clusters),
     lists:sort(
       ?projection(
          cluster,
          ?of_kind(classy_create_new_cluster, Trace)))).

end_per_testcase(_, _) ->
  snabbkaffe:stop().
