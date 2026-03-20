%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Text fixture that forwards snabbkaffe logs from the node.
-module(classy_test_snabbkaffe).

-behavior(classy_test_fixture).

%% behavior callbacks:
-export([init_per_node/4]).

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
  ok = snabbkaffe:forward_trace(Node),
  {ok, State}.
