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
        } = classy_ct:rpc(N1, classy_node, hello, []),
       ?assertMatch(ok, classy_ct:rpc(N2, classy, join, [maps:get(node, N1)])),
       [?block_until(
           #{ ?snk_kind := classy_member_join
            , cluster := Cluster1
            , site := Site2
            , ?snk_meta := #{node := Node}
            })
        || #{node := Node} <- Nodes],
       Nodes
     after
       classy_ct:teardown_cluster(Cluster)
     end,
     [ fun all_nodes_initialized_once/2
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

all_nodes_initialized_once(Cluster, Trace) ->
  ?assertEqual(
     lists:sort([N || #{node := N} <- Cluster]),
     lists:sort(
       ?projection(
          node,
          ?of_kind(classy_on_node_init, Trace)))).
