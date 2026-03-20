%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_test_cover).

-behavior(classy_test_fixture).

%% behavior callbacks:
-export([init_per_node/4, cleanup_per_node/4]).

-export_type([conf/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type conf() :: #{}.

%%================================================================================
%% behavior callbacks
%%================================================================================

init_per_node(_Site, Node, _Conf, State) ->
  {ok, _} = cover:start([Node]),
  {ok, State}.

cleanup_per_node(_Site, Node, _Conf, _State) ->
  cover:stop([Node]).
