%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_rt).
-moduledoc """
This module provides a layer of isolation from the runtime
(filesystem, time, RPC, ...) to allow for model checking using Concuerror.
""".

%% API:
-export([open_log/2, close_log/1]).
-export([get_membership_pid/3])
-export([time_s/1]).

-export_type([cbs/0]).

-include("classy_rt.hrl").

%%================================================================================
%% Behavior definition:
%%================================================================================

-type log() :: term().

-callback open_log(classy_pstore:name(), Suffix :: string()) -> {ok, log()} | {error, enoent | _}.

-callback close_log(log()) -> _.

-callback write_log(log(), term()) -> ok.

-doc """
Get local unix time (in seconds).
""".
-callback classy_time_s() -> non_neg_integer().

%%================================================================================
%% API functions
%%================================================================================

-spec open_log(module(), classy_pstore:name(), string()) -> {ok, cbs()}.
open_log(Mod, Name, Suffix) ->
  Mod:open_log(Name, Suffix).

-spec terminate(module(), cbs()) -> ok.
terminate(Mod, CBS) ->
  Mod:classy_terminate(CBS),
  ok.

-spec get_membership_pid(module(), classy:cluster_id(), classy:site()) -> pid() | undefined.
get_membership_pid(Mod, Cluster, Site) ->
  Mod:classy_get_membership_pid(Cluster, Site).

-spec time_s(module()) -> non_neg_integer().
time_s(Mod) ->
  Mod:classy_time_s().
