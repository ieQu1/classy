%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_table_tests).

-export([setup/1, cleanup/1]).

-include_lib("eunit/include/eunit.hrl").

%%================================================================================
%% Tests
%%================================================================================

%% This test verifies idempotency of `open' and `stop' functions.
smoke_open_test() ->
  Clean = setup(?FUNCTION_NAME),
  try
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:stop(t, 1000)),
    ?assertEqual(ok, classy_table:stop(t, 1000)),
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:open(t, #{}))
  after
    classy_table:drop(t),
    cleanup(Clean)
  end.

%% This test verifies effects of write and delete operations.
smoke_write_delete_test() ->
  Clean = setup(?FUNCTION_NAME),
  try
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:write(t, foo, foo)),
    ?assertEqual(ok, classy_table:write(t, foo, bar)),
    ?assertEqual([bar], classy_table:lookup(t, foo)),
    ?assertEqual(ok, classy_table:delete(t, foo)),
    ?assertEqual([], classy_table:lookup(t, foo))
  after
    classy_table:drop(t),
    cleanup(Clean)
  end.

%% This test verifies effects of dirty write and delete operations.
smoke_dirty_write_delete_test() ->
  Clean = setup(?FUNCTION_NAME),
  try
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:dirty_write(t, foo, foo)),
    ?assertEqual(ok, classy_table:dirty_write(t, foo, bar)),
    ?assertEqual([bar], classy_table:lookup(t, foo)),
    ?assertEqual(ok, classy_table:dirty_delete(t, foo)),
    ?assertEqual([], classy_table:lookup(t, foo))
  after
    classy_table:drop(t),
    cleanup(Clean)
  end.

%% This test verifies restoration of the log after combined dirty writes and deletes.
smoke_restore_test() ->
  Clean = setup(?FUNCTION_NAME),
  try
    ?assertEqual(ok, classy_table:open(t, #{})),
    %% Set `foo' to 1000:
    [?assertEqual(ok, classy_table:dirty_write(t, foo, N)) || N <- lists:seq(1, 100)],
    %% Set `bar' to 1000:
    [?assertEqual(ok, classy_table:write(t, bar, N)) || N <- lists:seq(1, 100)],
    %% Delete `baz' after setting it:
    ?assertEqual(ok, classy_table:dirty_write(t, baz, 1)),
    ?assertEqual(ok, classy_table:delete(t, baz)),
    %% Reopen table:
    ?assertEqual(ok, classy_table:stop(t, infinity)),
    ?assertEqual(ok, classy_table:open(t, #{})),
    %% Check values:
    ?assertEqual([100], classy_table:lookup(t, foo)),
    ?assertEqual([100], classy_table:lookup(t, bar)),
    ?assertEqual([], classy_table:lookup(t, baz))
  after
    classy_table:drop(t),
    cleanup(Clean)
  end.

%% This test verifies snapshot restoration.
smoke_snapshot_test() ->
  Clean = setup(?FUNCTION_NAME),
  try
    %% Insert data:
    ?assertEqual(ok, classy_table:open(t, #{})),
    [?assertEqual(ok, classy_table:dirty_write(t, N, N)) || N <- lists:seq(1, 100)],
    %% Checkpoint and reopen table:
    ?assertEqual(ok, classy_table:checkpoint(t)),
    ?assertEqual(ok, classy_table:stop(t, infinity)),
    ?assertEqual(ok, classy_table:open(t, #{})),
    %% Verify data:
    [?assertEqual([N], classy_table:lookup(t, N)) || N <- lists:seq(1, 100)]
  after
    classy_table:drop(t),
    cleanup(Clean)
  end.

%%================================================================================
%% Helper functions
%%================================================================================

-record(cleanup,
        { dir
        , apps
        }).

setup(TC) ->
  Dir = dir(TC),
  application:set_env(classy, table_dir, Dir),
  application:set_env(classy, table_batch_size, 10),
  ok = filelib:ensure_path(Dir),
  {ok, Apps} = application:ensure_all_started(classy),
  #cleanup{ dir = Dir
          , apps = Apps
          }.

cleanup(#cleanup{dir = Dir, apps = Apps}) ->
  [application:stop(A) || A <- lists:reverse(Apps)],
  ok = file:del_dir_r(Dir).

dir(TC) ->
  filename:join("_build/test_data", atom_to_list(TC)).
