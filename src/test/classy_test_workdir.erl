%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A test fixture that creates and optionally destroys the working directories of the sites.
%%
%% By default, working directories are deleted when the cluster is stopped with reason
%% `shutdown' or `normal'.
%%
%% This behavior can be overridden by setting a Linux environment variable `CLASSY_WORKDIR_CLEANUP':
%% <itemize>
%% <li>`true': Always delete</li>
%% <li>`false': Never delete</li>
%% <li>Other: default behavior</li>
%% </itemize>
%%
%% Configuration:
%% <itemize>
%% <li>`testcase': Name of the testcase or any other unique atom identifying the cluster.
%% Mandatory.</li>
%% </itemize>
-module(classy_test_workdir).

-behavior(classy_test_fixture).

%% API:
-export([]).

%% behavior callbacks:
-export([ init_per_cluster/2
        , cleanup_per_cluster/3
        , init_per_site/3
        , init_per_node/4
        ]).

-export_type([conf/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type conf() ::
        #{ testcase := atom()
         }.

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

%% @private
init_per_cluster(Conf, State) ->
  Suffix = case Conf of
             #{testcase := TC} -> [TC];
             _                 -> []
           end,
  Timestamp = integer_to_binary(os:system_time(second)),
  {ok, CWD} = file:get_cwd(),
  WD = filename:join([CWD, ?MODULE] ++ Suffix ++ [Timestamp]),
  ok = filelib:ensure_path(WD),
  {ok, State#{workdir => WD}}.

%% @private
cleanup_per_cluster(_Conf, Success, #{workdir := WD}) ->
  DoClean = case os:getenv("CLASSY_WORKDIR_CLEANUP") of
              "false" -> false;
              "true"  -> true;
              _       -> Success
            end,
  case DoClean of
    true ->
      logger:notice("Cleaning up working directory ~s", [WD]),
      file:del_dir_r(WD);
    false ->
      logger:notice("Keeping working directory ~s", [WD]),
      ok
  end.

%% @private
init_per_site(Site, _Conf, State = #{workdir := WDC}) ->
  WDS = classy_lib:ensure_list(filename:join(WDC, Site)),
  ok = filelib:ensure_path(WDS),
  {ok, State#{workdir := WDS}}.

%% @private
init_per_node(Site, _Node, _Conf, State = #{workdir := WD}) ->
  case classy_test_site:call(Site, file, set_cwd, [WD]) of
    ok ->
      {ok, State};
    Err ->
      Err
  end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
