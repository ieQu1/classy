%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Misc. utility functions
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

        , sync_stop_proc/3
        , ensure_list/1
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

%% @doc Read `rpc_timeout' environment variable (with default)
rpc_timeout() ->
  application:get_env(classy, rpc_timeout, 5_000).

%% @doc Read `n_sites' environment variable (with default)
n_sites() ->
  application:get_env(classy, n_sites, 5_000).

%% @doc Adjust a local timestamp `Val' to the remote nodes's clock,
%% given the remote's "current" time `RemoteTimeS' at the time of the
%% call.
adjust_time_s_skew(RemoteTimeS, Val) ->
  Skew = RemoteTimeS - time_s(),
  Val + Skew.

-ifndef(CONCUERROR).

%% @doc Return Unix time in seconds.
time_s() ->
  os:system_time(second).

-endif.

%% @doc Set up a wakeup timer that sends message `Msg' to the calling process.
%%
%% If the timer was previously set up to fire at a later time,
%% this function resets it to the earlier time.
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

%% @doc Send exit signal `Reason' to a process and wait for the shutdown.
-spec sync_stop_proc(pid() | atom(), _ExitReason, timeout()) -> ok.
sync_stop_proc(undefined, _, _) ->
  ok;
sync_stop_proc(Name, Reason, Timeout) when is_atom(Name) ->
  sync_stop_proc(whereis(Name), Reason, Timeout);
sync_stop_proc(Pid, Reason, Timeout) when is_pid(Pid) ->
  unlink(Pid),
  MRef = monitor(process, Pid),
  exit(Pid, Reason),
  receive
    {'DOWN', MRef, process, _, _} ->
      ok
  after Timeout ->
      {error, timeout}
  end.

%% @doc If input is a binary, convert it to a list.
%% Keep input list as is.
-spec ensure_list(binary() | string()) -> string().
ensure_list(L) when is_list(L) ->
  L;
ensure_list(Bin) when is_binary(Bin) ->
  binary_to_list(Bin).

%%================================================================================
%% Internal functions
%%================================================================================
