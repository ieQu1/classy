%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A gen_server that implements automatic peer discovery.
-module(classy_autocluster).

-behavior(gen_server).

%% API:
-export([ start_link/0
        , enable/0
        , disable/0
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(cast_enable, {enable :: boolean()}).
-record(to_discover, {}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec enable() -> ok.
enable() ->
  gen_server:cast(?SERVER, #cast_enable{enable = true}).

-spec disable() -> ok.
disable() ->
  gen_server:cast(?SERVER, #cast_enable{enable = false}).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { t :: classy_lib:wakeup_timer()
        }).

init(_) ->
  process_flag(trap_exit, true),
  S = #s{},
  {ok, S}.

handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

handle_cast(#cast_enable{enable = Enable}, S0 = #s{t = T}) ->
  S = case Enable of
        true  -> wakeup(0, S0);
        false -> S0#s{t = classy_lib:cancel_wakeup(T)}
      end,
  {noreply, S};
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

handle_info(#to_discover{}, S) ->
  {noreply, handle_discover(S#s{t = undefined})};
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

terminate(Reason, S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

handle_discover(S0) ->
  S = S0,
  wakeup(S).

-spec wakeup(#s{}) -> #s{}.
wakeup(S) ->
  wakeup(discovery_interval(), S).

-spec wakeup(non_neg_integer(), #s{}) -> #s{}.
wakeup(After, S = #s{t = T0}) ->
  T = classy_lib:wakeup_after(#to_discover{}, After, T0),
  S#s{t = T}.

-spec discovery_interval() -> pos_integer().
discovery_interval() ->
  application:get_env(classy, discovery_interval, 5_000).
