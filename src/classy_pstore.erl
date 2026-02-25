%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_pstore).
-moduledoc """
This module implements a standalone minimalist analogue of mnesia's `disc_copies` storage.
""".

-behavior(gen_server).

%% API:
-export([ open/2
        , write/3
        , dirty_write/3
        , flush/1
        , lookup/2
        ]).

%% behavior callbacks:
-export([ init/1
        , handle_continue/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

%% internal exports:
-export([start_link/3]).

-export_type([tab/0, rec/0, options/0]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(name(TAB), {n, l, {?MODULE, TAB}}).
-define(via(TAB), {via, gproc, ?name(TAB)}).

-type tab() :: atom().

-type options() :: #{
                    }.

-type rec() :: #classy_pstore{ k :: term()
                             , v :: term()
                             , dirty :: boolean()
                             }.

-record(call_ensure_open, {tab :: tab()}).
-record(call_write, {k, v, wal = true :: boolean()}).
-record(call_flush, {}).
-record(call_snapshot, {}).

%%================================================================================
%% API functions
%%================================================================================

-spec open(tab(), options()) -> ok | {error, _}.
open(Tab, Options) ->
  maybe
    {ok, _} ?=
      case gproc:where(?name(Tab)) of
        undefined ->
          classy_sup:start_pstore(Tab, Options);
        Pid when is_pid(Pid) ->
          {ok, Pid}
      end,
    gen_server:call(?via(Tab), #call_ensure_open{tab = Tab})
  end.

-doc """
Update the RAM representation of the record and mark it as dirty.
No writes to disk are made until `flush` is called explicitly or implicitly.
""".
-spec dirty_write(tab(), _Key, _Val) -> ok.
dirty_write(Tab, Key, Val) ->
  gen_server:call(?via(Tab), #call_write{k = Key, v = Val, wal = false}).

-doc """
Write operation to WAL and update RAM representation of the record.
""".
-spec write(tab(), _Key, _Val) -> ok.
write(Tab, Key, Val) ->
  gen_server:call(?via(Tab), #call_write{k = Key, v = Val, wal = true}).

-doc """
Persist all records that got dirtied prior to this call to WAL.
""".
-spec flush(tab()) -> ok.
flush(Tab) ->
  gen_server:call(?via(Tab), #call_flush{}).

-spec lookup(tab(), _Key) -> [_Val].
lookup(Tab, Key) ->
  [V || #classy_pstore{v = V} <- ets:lookup(Tab, Key)].

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link(module(), tab(), options()) -> {ok, pid()}.
start_link(RT, Tab, Options) ->
  gen_server:start_link(?via(Tab), ?MODULE, [RT, Tab, Options], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { rt :: module()
        , name :: tab()
        , ets :: ets:tid()
        }).

init([RTMod, TabName, Options]) ->
  process_flag(trap_exit, true),
  ETSOpts = maps:get(ets_options, Options, [set]),
  S = #s{ rt = RTMod
        , name = TabName
        , ets = ets:new(TabName, [named_table, protected, {keypos, #classy_pstore.k} | ETSOpts])
        },
  {ok, S, {continue, restore}}.

handle_continue(restore, S = #s{name = Name, rt = RT, ets = ETS}) ->
  ok = classy_rt:pstore_restore(RT, Name, ETS),
  {noreply, S}.

handle_call(#call_ensure_open{}, _From, S) ->
  {reply, ok, S};
handle_call(#call_write{} = C, _From, S) ->
  {reply, ok, handle_write(C, S)};
handle_call(#call_flush{}, _From, S) ->
  {reply, ok, handle_flush(S)};
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
  {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, S) ->
  handle_flush(S),
  ok.

%%================================================================================
%% Internal functions
%%================================================================================

handle_write(#call_write{k = K, v = V, wal = Wal}, S = #s{rt = RT, name = Name, ets = ETS}) ->
  Wal andalso classy_rt:append_wal(RT, Name, K, V),
  Dirty = not Wal,
  ets:insert(ETS, #classy_pstore{k = K, v = V, dirty = Dirty}),
  S.

handle_flush(S) ->
  %% FIXME
  S.
