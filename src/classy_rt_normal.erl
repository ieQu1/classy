%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_rt_normal).

-behavior(classy_rt).

-export([ classy_has_log/1
        , classy_open_log/2
        , classy_close_log/1
        , classy_write_log/2
        , classy_log_chunk/3
        , classy_time_s/0
        , classy_get_membership_pid/2
        ]).

-export_type([log/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type log() :: term().
-type log_cont() :: disk_log:continuation().

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec classy_has_log(file:filename()) -> boolean().
classy_has_log(Filename) ->
  filelib:is_file(Filename).

-spec classy_open_log(file:filename(), read_only | read_write) -> {ok, log()} | {error, no_such_log | _}.
classy_open_log(Filename, Mode) ->
  Opts = [ {name, Filename}
         , {file, Filename}
         , {mode, Mode}
         , {format, internal}
         , {type, halt}
         , {repair, true}
         , {notify, false}
         ],
  case disk_log:open(Opts) of
    {ok, Log} ->
      {ok, Log};
    {repaired, Log, _Recovered, _BadBytes} ->
      {ok, Log};
    {error, Reason} ->
      {error, Reason}
  end.

-spec classy_close_log(log()) -> ok.
classy_close_log(Log) ->
  disk_log:close(Log).

-spec classy_write_log(log(), [term()]) -> ok | {error, _}.
classy_write_log(Log, Terms) ->
  disk_log:log_terms(Log, Terms).

-spec classy_log_chunk(log(), log_cont() | start, pos_integer()) -> {ok, log_cont(), [term()]} | eof | {error, _}.
classy_log_chunk(Log, Cont, Size) ->
  case disk_log:chunk(Log, Cont, Size) of
    {error, _} = Err ->
      Err;
    {NewCont, Terms} ->
      {ok, NewCont, Terms};
    {NewCont, Terms, _BadBytes} ->
      %% In case of corrupt data in read-only mode, we still return what we can
      {ok, NewCont, Terms};
    eof ->
      eof
  end.

-spec classy_time_s() -> non_neg_integer().
classy_time_s() ->
  os:system_time(second).

%%--------------------------------------------------------------------
%% Cluster Interface
%%--------------------------------------------------------------------

%% Get the membership process PID for a cluster and site
-spec classy_get_membership_pid(classy:cluster_id(), classy:site()) -> pid() | undefined.
classy_get_membership_pid(_Cluster, _Site) ->
    error(todo).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
