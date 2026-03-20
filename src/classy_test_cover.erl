%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Test fixture that adds code coverage capture to the child nodes.
%%
%% There's no configuration.
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

%% @private
init_per_node(_Site, Node, _Conf, State) ->
  {ok, _} = cover:start([Node]),
  {ok, State}.

%% @private
cleanup_per_node(_Site, Node, _Conf, _State) ->
  cover:stop([Node]).
