%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_start_system_fixture).

-behavior(familiar_fixture).

%% behavior callbacks:
-export([init_per_node/4, cleanup_per_node/5]).

-export_type([conf/0]).

-type conf() :: #{timeout => timeout()}.

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec init_per_node(familiar:site(), node(), conf(), familiar_fixture:state()) -> {ok, familiar_fixture:state()}.
init_per_node(Site, _Node, Conf, State) ->
  ok = familiar:call(Site, classy, start_system, [], timeout(Conf)),
  {ok, State}.

-spec cleanup_per_node(familiar:site(), node(), _, familiar_fixture:state(), IsKill) -> ok | {error, _}
  when IsKill :: boolean().
cleanup_per_node(Site, _Node, Conf, _State, false) ->
  ok = familiar:call(Site, classy, stop_system, [], timeout(Conf));
cleanup_per_node(_, _, _, _, true) ->
  ok.

-spec timeout(conf()) -> timeout().
timeout(#{timeout := TO}) ->
  TO;
timeout(#{}) ->
  5_000.
