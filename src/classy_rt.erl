%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_rt).
-moduledoc """
This module provides a layer of isolation from the runtime
(filesystem, time, RPC, ...) to allow for model checking using Concuerror.
""".

%% API:
-export([has_log/2, open_log/3, log_chunk/4, log_write/3, close_log/2]).
-export([get_membership_pid/3]).
-export([time_s/1]).

-export_type([log/0, log_cont/0]).

-include("classy_rt.hrl").

%%================================================================================
%% Behavior definition:
%%================================================================================

-opaque log() :: term().
-opaque log_cont() :: term().

-callback classy_has_log(file:filename()) -> boolean().

-callback classy_open_log(file:filename(), read_only | read_write) -> {ok, log()} | {error, enoent | _}.

-callback classy_close_log(log()) -> _.

-callback classy_write_log(log(), [term()]) -> ok.

-doc """
Get local unix time (in seconds).
""".
-callback classy_time_s() -> non_neg_integer().

%%================================================================================
%% API functions
%%================================================================================

-spec has_log(module(), file:filename()) -> boolean().
has_log(Mod, Filename) ->
  Mod:classy_has_log(Filename).

-spec open_log(module(), file:filename(), read_only | read_write) -> {ok, log()} | {error, _Reason}.
open_log(Mod, Filename, Mode) ->
  Mod:classy_open_log(Filename, Mode).

-spec log_chunk(module(), log(), log_cont() | start, pos_integer()) -> {ok, log_cont(), [term()]} | eof | {error, _}.
log_chunk(Mod, Log, Cont, Size) ->
  Mod:classy_log_chunk(Log, Cont, Size).

-spec log_write(module(), log(), list()) -> ok | {error, _}.
log_write(Mod, Log, Terms) ->
  Mod:classy_write_log(Log, Terms).

-spec close_log(module(), log()) -> ok.
close_log(Mod, Log) ->
  Mod:classy_close_log(Log),
  ok.

-spec get_membership_pid(module(), classy:cluster_id(), classy:site()) -> pid() | undefined.
get_membership_pid(Mod, Cluster, Site) ->
  Mod:classy_get_membership_pid(Cluster, Site).

-spec time_s(module()) -> non_neg_integer().
time_s(Mod) ->
  Mod:classy_time_s().
