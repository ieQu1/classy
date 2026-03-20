%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
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

init_per_cluster(#{testcase := TC}, State) ->
  {ok, CWD} = file:get_cwd(),
  WD = filename:join([CWD, "classy_cluster", TC]),
  ok = filelib:ensure_path(WD),
  {ok, State#{workdir => WD}}.

cleanup_per_cluster(_Conf, Success, #{workdir := WD}) ->
  DoClean = case os:getenv("CLASSY_WORKDIR_CLEANUP") of
              "false" -> false;
              "true"  -> true;
              _       -> not Success
            end,
  case DoClean of
    true  -> file:del_dir_r(WD);
    false -> ok
  end.

init_per_site(Site, _Conf, State = #{workdir := WDC}) ->
  WDS = classy_lib:ensure_list(filename:join(WDC, Site)),
  ok = filelib:ensure_path(WDS),
  {ok, State#{workdir := WDS}}.

init_per_node(Site, _Node, _Conf, State = #{workdir := WD}) ->
  case classy_test_site:call(Site, fun() -> file:set_cwd(WD) end) of
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
