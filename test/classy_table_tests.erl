%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_table_tests).

-include_lib("eunit/include/eunit.hrl").

%%================================================================================
%% Tests
%%================================================================================

%% This test verifies idempotency of `open' and `stop' functions.
smoke_open_test() ->
  try
    setup(?FUNCTION_NAME),
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:stop(t, 1000)),
    ?assertEqual(ok, classy_table:stop(t, 1000)),
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:open(t, #{}))
  after
    cleanup(?FUNCTION_NAME)
  end.

%% This test verifies effects of write and delete operations.
smoke_write_delete_test() ->
  try
    setup(?FUNCTION_NAME),
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:write(t, foo, foo)),
    ?assertEqual(ok, classy_table:write(t, foo, bar)),
    ?assertEqual([bar], classy_table:lookup(t, foo)),
    ?assertEqual(ok, classy_table:delete(t, foo)),
    ?assertEqual([], classy_table:lookup(t, foo))
  after
    cleanup(?FUNCTION_NAME)
  end.

%% This test verifies effects of dirty write and delete operations.
smoke_dirty_write_delete_test() ->
  try
    setup(?FUNCTION_NAME),
    ?assertEqual(ok, classy_table:open(t, #{})),
    ?assertEqual(ok, classy_table:dirty_write(t, foo, foo)),
    ?assertEqual(ok, classy_table:dirty_write(t, foo, bar)),
    ?assertEqual([bar], classy_table:lookup(t, foo)),
    ?assertEqual(ok, classy_table:dirty_delete(t, foo)),
    ?assertEqual([], classy_table:lookup(t, foo))
  after
    cleanup(?FUNCTION_NAME)
  end.

%% This test verifies restoration of the log after combined dirty writes and deletes.
smoke_restore_test() ->
  try
    setup(?FUNCTION_NAME),
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
    cleanup(?FUNCTION_NAME)
  end.

%% This test verifies snapshot restoration.
smoke_snapshot_test() ->
  try
    setup(?FUNCTION_NAME),
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
    cleanup(?FUNCTION_NAME)
  end.

%%================================================================================
%% Helper functions
%%================================================================================

setup(FunctionName) ->
  Dir = dir(FunctionName),
  application:set_env(classy, table_dir, Dir),
  application:set_env(classy, table_batch_size, 10),
  filelib:ensure_path(Dir),
  application:ensure_all_started(classy).

cleanup(FunctionName) ->
  file:del_dir_r(dir(FunctionName)),
  application:stop(classy).

dir(FunctionName) ->
  filename:join("_build/test_data", atom_to_list(FunctionName)).
