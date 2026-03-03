%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc
%% This module implements a standalone minimalist analogue of mnesia's `local_data` table with `disc_copies` storage.
%%
%% It is used to persistently save classy's own data.
%% Other applications can also use it for data that doesn't require replication and is not written too frequently.
-module(classy_table).

-behavior(gen_server).

%% API:
-export([ open/2
        , stop/2
        , write/3
        , dirty_write/3
        , delete/2
        , dirty_delete/2
        , flush/1
        , checkpoint/1
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

-type options() :: #{}.

-type rec() :: #classy_kv{ k :: term()
                             , v :: term()
                             }.

-record(call_ensure_open, {tab :: tab()}).
-record(call_write, {k, v, wal = true :: boolean()}).
-record(call_delete, {k, wal = true :: boolean()}).
-record(call_flush, {}).
-record(call_checkpoint, {}).

-define(w(K, V), {w, K, V}).
-define(d(K), {d, K}).

%%================================================================================
%% API functions
%%================================================================================


%% @doc Open a table named `Tab'.
%%
%% Note: this function is idempotent.
-spec open(tab(), options()) -> ok | {error, _}.
open(Tab, Options) ->
  case classy_sup:start_table(Tab, Options) of
    {ok, Pid} ->
      gen_server:call(Pid, #call_ensure_open{tab = Tab}, infinity);
    {error, {already_started, Pid}} ->
      gen_server:call(Pid, #call_ensure_open{tab = Tab}, infinity);
    Err = {error, _} ->
      Err
  end.

-spec stop(tab(), timeout()) -> ok | {error, timeout}.
stop(Tab, Timeout) ->
  case gproc:where(?name(Tab)) of
    Pid when is_pid(Pid) ->
      MRef = monitor(process, Pid),
      erlang:exit(Pid, shutdown),
      receive
        {'DOWN', MRef, process, _, _} ->
          ok
      after Timeout ->
          {error, timeout}
      end;
    undefined ->
      ok
  end.

%% @doc Update the RAM representation of the record and mark it as dirty.
%% No writes to disk are made until `flush' is called explicitly or implicitly.
-spec dirty_write(tab(), _Key, _Val) -> ok.
dirty_write(Tab, Key, Val) ->
  gen_server:call(?via(Tab), #call_write{k = Key, v = Val, wal = false}).

%% @doc Write operation to WAL and update RAM representation of the record.
-spec write(tab(), _Key, _Val) -> ok.
write(Tab, Key, Val) ->
  gen_server:call(?via(Tab), #call_write{k = Key, v = Val, wal = true}).

%% @doc Mark record as deleted and dirty.
-spec dirty_delete(tab(), _Key) -> ok.
dirty_delete(Tab, Key) ->
  gen_server:call(?via(Tab), #call_delete{k = Key, wal = false}).

%% @doc Write to the WAL that the record has been deleted and update the RAM representation.
-spec delete(tab(), _Key) -> ok.
delete(Tab, Key) ->
  gen_server:call(?via(Tab), #call_delete{k = Key, wal = true}).

%% @doc Persist all records that got dirtied prior to this call to WAL.
-spec flush(tab()) -> ok.
flush(Tab) ->
  gen_server:call(?via(Tab), #call_flush{}, infinity).

%% @doc Make a checkpoint and trunkate the WAL.
-spec checkpoint(tab()) -> ok.
checkpoint(Tab) ->
  gen_server:call(?via(Tab), #call_checkpoint{}, infinity).

-spec lookup(tab(), _Key) -> [_Val].
lookup(Tab, Key) ->
  [V || #classy_kv{v = V} <- ets:lookup(Tab, Key)].

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
        , dir :: file:filename()
        , dirty :: #{_ => true}
        , log :: classy_rt:log() | undefined
        }).

-type s() :: #s{}.

init([RTMod, TabName, Options]) ->
  process_flag(trap_exit, true),
  ETSOpts = maps:get(ets_options, Options, [set]),
  S = #s{ rt = RTMod
        , name = TabName
        , ets = ets:new(TabName, [named_table, protected, {keypos, #classy_kv.k} | ETSOpts])
        , dirty = #{}
        , dir = application:get_env(classy, table_dir, ".")
        },
  {ok, S, {continue, restore}}.

handle_continue(restore, S) ->
  {noreply, restore(S)}.

handle_call(#call_ensure_open{}, _From, S) ->
  {reply, ok, S};
handle_call(#call_write{} = C, _From, S) ->
  {reply, ok, handle_write(C, S)};
handle_call(#call_delete{} = C, _From, S) ->
  {reply, ok, handle_delete(C, S)};
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

-spec restore(s()) -> s().
restore(S = #s{rt = RT, ets = ETS}) ->
  RegularName = log_name(S, ""),
  NewName = log_name(S, ".NEW"),
  ets:match_delete(ETS, '_'),
  case {classy_rt:has_log(RT, RegularName), classy_rt:has_log(RT, NewName)} of
    {false, false} ->
      {ok, Log} = classy_rt:open_log(RT, RegularName, read_write),
      S#s{log = Log};
    {true, false} ->
      %% Normal case:
      {ok, Log} = classy_rt:open_log(RT, RegularName, read_write),
      do_restore(RT, Log, start, ETS),
      S#s{log = Log};
    _ ->
      %% TODO: handle aborted checkpoint
      error(todo)
  end.

do_restore(RT, Log, Cont0, ETS) ->
  case classy_rt:log_chunk(RT, Log, Cont0, 100) of
    {ok, Cont, Chunk} ->
      lists:foreach(
        fun(?w(K, V)) ->
            ets:insert(ETS, #classy_kv{k = K, v = V});
           (?d(K)) ->
            ets:delete(ETS, K)
        end,
        Chunk),
      do_restore(RT, Log, Cont, ETS);
    eof ->
      ok
  end.

-spec log_name(s(), string()) -> file:filename().
log_name(#s{name = Name, dir = Dir}, Suffix) ->
  FN = atom_to_list(Name) ++ Suffix,
  filename:join(Dir, FN).

handle_write(#call_write{k = K, v = V, wal = true}, S = #s{rt = RT, ets = ETS, log = Log}) ->
  ok = classy_rt:log_write(RT, Log, [?w(K, V)]),
  ets:insert(ETS, #classy_kv{k = K, v = V}),
  S;
handle_write(#call_write{k = K, v = V, wal = false}, S = #s{ets = ETS, dirty = D0}) ->
  ets:insert(ETS, #classy_kv{k = K, v = V}),
  S#s{dirty = D0#{K => true}}.

handle_delete(#call_delete{k = K, wal = true}, S = #s{rt = RT, ets = ETS, log = Log}) ->
  ok = classy_rt:log_write(RT, Log, [?d(K)]),
  ets:delete(ETS, K),
  S;
handle_delete(#call_delete{k = K, wal = false}, S = #s{ets = ETS, dirty = D0}) ->
  ets:delete(ETS, K),
  S#s{dirty = D0#{K => true}}.

handle_flush(S = #s{dirty = Dirty}) when map_size(Dirty) =:= 0 ->
  S;
handle_flush(S = #s{rt = RT, ets = ETS, log = Log, dirty = Dirty}) ->
  Ops = maps:fold(
          fun(K, _, Acc) ->
              case ets:lookup(ETS, K) of
                [#classy_kv{v = V}] ->
                  [?w(K, V) | Acc];
                [] ->
                  [?d(K) | Acc]
              end
          end,
          [],
          Dirty),
  ok = classy_rt:log_write(RT, Log, Ops),
  S#s{dirty = #{}}.
