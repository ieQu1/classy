%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_test_code_path).

-behavior(classy_test_fixture).

%% behavior callbacks:
-export([ init_per_cluster/2
        , init_per_node/4
        ]).

-export_type([conf/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type conf() :: #{code_paths => [file:filename()]}.

%%================================================================================
%% behavior callbacks
%%================================================================================

init_per_cluster(Conf, State) ->
  CP = maps:get(code_paths, Conf, code:get_path()),
  {ok, State#{code_paths => CP}}.

init_per_node(Site, _Node, _Conf, State = #{code_paths := CP}) ->
  classy_test_site:call(
    Site,
    fun() ->
        lists:foreach(fun code:add_patha/1, CP)
    end),
  {ok, State}.
