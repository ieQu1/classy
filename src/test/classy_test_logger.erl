%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Test fixture that configures logger.
%%
%% It must run after `classy_test_workdir'
-module(classy_test_logger).

-behavior(classy_test_fixture).

%% behavior callbacks:
-export([init_per_cluster/2, init_per_node/4, cleanup_per_node/4]).

-export_type([conf/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type conf() :: #{}.

%%================================================================================
%% behavior callbacks
%%================================================================================

%% @private
init_per_cluster(_TC, State = #{workdir := _}) ->
  {ok, State};
init_per_cluster(_, _State) ->
  error(logger_needs_work_dir).

%% @private
init_per_node(Site, _Node, Conf, State) ->
  #{workdir := _WorkDir} = State,
  LogFile = "erlang.log",
  Level = maps:get(level, Conf, debug),
  HandlerConf = #{ level => Level
                 , filter_default => log
                 , config => #{ type => file
                              , file => LogFile
                              }
                 , formatter => {logger_formatter, #{ single_line => false
                                                    , legacy_header => true
                                                    }}
                 },
  ok = classy_test_site:call(
         Site,
         logger, update_primary_config, [#{level => Level}]),
  ok = classy_test_site:call(
         Site,
         logger, add_handler, [?MODULE, logger_std_h, HandlerConf]),
  {ok, State#{log_file => LogFile}}.

%% @private
cleanup_per_node(Site, _Node, _Conf, #{log_file := LogFile}) ->
  classy_test_site:call(
    Site,
    logger_std_h, filesync, [LogFile]).
