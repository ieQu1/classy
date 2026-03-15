%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_lib).

%% API:
-export([]).

%% internal exports:
-export([ rpc_timeout/0
        , n_sites/0
        , time_s/0
        , adjust_time_s_skew/2

        , wakeup_after/3
        , cancel_wakeup/1
        ]).

-export_type([unix_time_s/0, wakeup_timer/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type unix_time_s() :: integer().

-type wakeup_timer() :: undefined | {integer(), reference()}.

%%================================================================================
%% API functions
%%================================================================================

rpc_timeout() ->
  application:get_env(classy, rpc_timeout, 5_000).

n_sites() ->
  application:get_env(classy, n_sites, 5_000).

%% @doc Adjust a local timestamp `Val' to the remote nodes's clock,
%% given the remote's "current" time `RemoteTimeS' at the time of the
%% call.
adjust_time_s_skew(RemoteTimeS, Val) ->
  Skew = RemoteTimeS - time_s(),
  Val + Skew.

-ifndef(CONCUERROR).

time_s() ->
  os:system_time(second).

-endif.

-spec wakeup_after(term(), integer(), wakeup_timer()) -> wakeup_timer().
wakeup_after(Msg, After, undefined) ->
  { erlang:monotonic_time(millisecond) + After
  , erlang:send_after(After, self(), Msg)
  };
wakeup_after(Msg, After, {OldDeadline, OldTRef} = Old) ->
  NewDeadline = erlang:monotonic_time(millisecond) + After,
  if OldDeadline > NewDeadline ->
      erlang:cancel_timer(OldTRef),
      { NewDeadline
      , erlang:send_after(After, self(), Msg)
      };
     true ->
      Old
  end.

-spec cancel_wakeup(wakeup_timer()) -> undefined.
cancel_wakeup(undefined) ->
  undefined;
cancel_wakeup({_, TRef}) ->
  erlang:cancel_timer(TRef),
  undefined.

%%================================================================================
%% Internal functions
%%================================================================================
