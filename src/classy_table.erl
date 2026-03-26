%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc
%% This module implements a standalone minimalist analogue of mnesia's `local_data' table with `disc_copies' storage.
%%
%% It is used to persistently save classy's own data.
%% Other applications can also use it for data that doesn't require replication and is not written too frequently.
%%
%% It is meant for small volumes of data and infrequent updates.
-module(classy_table).

-behavior(gen_server).

%% API:
-export([ open/2
        , stop/2
        , drop/1
        , write/3
        , dirty_write/3
        , delete/2
        , dirty_delete/2
        , flush/1
        , force_compaction/1
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
-export([start_link/2]).

-export_type([tab/0, rec/0, options/0]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(name(TAB), {n, l, {?MODULE, TAB}}).
-define(via(TAB), {via, gproc, ?name(TAB)}).

-type tab() :: atom().

-type options() ::
        #{ ets_options => list()
         , badness_threshold => pos_integer()
         }.

-type rec() :: #classy_kv{ k :: term()
                         , v :: term()
                         }.

-record(call_ensure_open, {tab :: tab()}).
-record(call_write, {k, v, wal = true :: boolean()}).
-record(call_delete, {k, wal = true :: boolean()}).
-record(call_flush, {}).
-record(call_force_compaction, {}).
-record(call_drop, {}).

-define(w(K, V), {w, K, V}).
-define(d(K), {d, K}).

%%================================================================================
%% API functions
%%================================================================================


%% @doc Open a table named `Tab'.
%%
%% Note: this function is idempotent.
-spec open(tab(), options()) -> ok | {error, _}.
open(Tab, Options) when is_atom(Tab), is_map(Options) ->
  case classy_sup:start_table(Tab, Options) of
    {ok, Pid} ->
      ensure_open(Pid, Tab);
    {error, {already_started, Pid}} ->
      ensure_open(Pid, Tab);
    Err = {error, _} ->
      Err
  end.

%% @doc Close the table.
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
-spec force_compaction(tab()) -> ok.
force_compaction(Tab) ->
  gen_server:call(?via(Tab), #call_force_compaction{}, infinity).

%% @doc Drop the table (it must be open)
-spec drop(tab()) -> ok.
drop(Tab) ->
  gen_server:call(?via(Tab), #call_drop{}, infinity).

%% @doc Lookup a value from the table.
-spec lookup(tab(), _Key) -> [_Val].
lookup(Tab, Key) ->
  [V || #classy_kv{v = V} <- ets:lookup(Tab, Key)].

%%================================================================================
%% Internal exports
%%================================================================================

%% @private
-spec start_link(tab(), options()) -> {ok, pid()}.
start_link(Tab, Options) ->
  gen_server:start_link(?via(Tab), ?MODULE, [Tab, Options], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { name :: tab()
        , ets :: ets:tid()
        , dir :: file:filename()
        , dirty :: #{_ => true}
        , log :: _
        , log_size = 0 :: non_neg_integer()
        , badness_threshold :: pos_integer()
        }).

-type s() :: #s{}.

%% @private
init([TabName, Options]) ->
  process_flag(trap_exit, true),
  ETSOpts = maps:get(ets_options, Options, [set]),
  BadnessThreshold = maps:get(badness_threshold, Options, 100),
  S = #s{ name = TabName
        , ets = ets:new(TabName, [named_table, protected, {keypos, #classy_kv.k} | ETSOpts])
        , dirty = #{}
        , dir = application:get_env(classy, table_dir, ".")
        , badness_threshold = BadnessThreshold
        },
  {ok, S, {continue, restore}}.

%% @private
handle_continue(restore, S) ->
  case restore(S) of
    {ok, S1} ->
      {noreply, S1};
    {stop, Reason, S1} ->
      {stop, Reason, S1}
  end.

%% @private
handle_call(#call_ensure_open{}, _From, S) ->
  {reply, ok, S};
handle_call(#call_write{} = C, _From, S) ->
  {reply, ok, handle_write(C, S)};
handle_call(#call_delete{} = C, _From, S) ->
  {reply, ok, handle_delete(C, S)};
handle_call(#call_flush{}, From, S) ->
  maybe_compact(From, ok, handle_flush(S));
handle_call(#call_force_compaction{}, From, S0) ->
  case do_compaction(S0) of
    {ok, S} ->
      {reply, ok, S};
    {error, Reason, S} ->
      gen_server:reply(From, {error, Reason}),
      {stop, compaction_failed, S}
  end;
handle_call(#call_drop{}, From, S) ->
  {stop, normal, handle_drop(From, S)};
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

%% @private
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

%% @private
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

%% @private
terminate(_, undefined) ->
  ok;
terminate(_Reason, S = #s{}) ->
  handle_flush(S),
  ok.

%%================================================================================
%% Internal functions
%%================================================================================

-spec restore(s()) -> {ok, s()} | {stop, _Reason, s()}.
restore(S = #s{ets = ETS}) ->
  RegularName = log_name(S, ""),
  NewName = log_name(S, ".NEW"),
  ets:match_delete(ETS, '_'),
  case {is_log(RegularName), is_log(NewName)} of
    {false, false} ->
      with_log_open(RegularName, read_write, S);
    {true, false} ->
      %% Normal case:
      case with_log_open(RegularName, read_write, S) of
        {ok, S1 = #s{log = Log}} ->
          case do_restore(Log, start, ETS, 0) of
            {ok, LogSize} ->
              {ok, S1#s{log_size = LogSize}};
            {error, Reason} ->
              log_restore_failure(S, RegularName, Reason),
              {stop, {classy_table_restore_failed, S#s.name, RegularName, Reason}, S1}
          end;
        {stop, _, _} = Err ->
          Err
      end;
    {true, true} ->
      %% Server was stopped while compaction was ongling:
      logger:warning(#{ msg => classy_table_aborted_compaction
                      , log_name => NewName
                      }),
      case rename_log(NewName, NewName ++ ".bup") of
        ok ->
          restore(S);
        {error, Reason} ->
          log_restore_failure(S, NewName, {rename_failed, Reason}),
          {stop, {classy_table_restore_failed, S#s.name, NewName, {rename_failed, Reason}}, S}
      end;
    {false, true} ->
      Reason = {classy_unrecoverable_aborted_table_compaction, NewName},
      log_restore_failure(S, NewName, Reason),
      {stop, Reason, S}
  end.

do_restore(Log, Cont0, ETS, N) ->
  case read_log_chunk(Log, Cont0, batch_size()) of
    {ok, Cont, Chunk} ->
      lists:foreach(
        fun(?w(K, V)) ->
            ets:insert(ETS, #classy_kv{k = K, v = V});
           (?d(K)) ->
            ets:delete(ETS, K)
        end,
        Chunk),
      do_restore(Log, Cont, ETS, N + length(Chunk));
    {error, Reason} ->
      {error, Reason};
    eof ->
      {ok, N}
  end.

-spec log_name(s(), string()) -> file:filename().
log_name(#s{name = Name, dir = Dir}, Suffix) ->
  FN = atom_to_list(Name) ++ Suffix,
  filename:join(Dir, FN).

handle_write(
  #call_write{k = K, v = V, wal = true},
  #s{ets = ETS, log = Log, dirty = D, log_size = LogSize} = S
 ) ->
  ok = write_log(Log, [?w(K, V)]),
  ets:insert(ETS, #classy_kv{k = K, v = V}),
  S#s{ dirty = maps:remove(K, D)
     , log_size = LogSize + 1
     };
handle_write(#call_write{k = K, v = V, wal = false}, S = #s{ets = ETS, dirty = D}) ->
  ets:insert(ETS, #classy_kv{k = K, v = V}),
  S#s{ dirty = D#{K => true}
     }.

handle_delete(
  #call_delete{k = K, wal = true},
  #s{ets = ETS, log = Log, dirty = D, log_size = LogSize} = S
 ) ->
  ok = write_log(Log, [?d(K)]),
  ets:delete(ETS, K),
  S#s{ dirty = maps:remove(K, D)
     , log_size = LogSize + 1
     };
handle_delete(#call_delete{k = K, wal = false}, S = #s{ets = ETS, dirty = D}) ->
  ets:delete(ETS, K),
  S#s{dirty = D#{K => true}}.

handle_flush(S = #s{log = Log, dirty = Dirty}) when Log =:= undefined;
                                                    map_size(Dirty) =:= 0 ->
  S;
handle_flush(S = #s{ets = ETS, log = Log, dirty = Dirty, log_size = LogSize0}) ->
  {LogSize, Ops} =
    maps:fold(
      fun(K, _, {AccNW, AccOps}) ->
          Op = case ets:lookup(ETS, K) of
                 [#classy_kv{v = V}] -> ?w(K, V);
                 []                  -> ?d(K)
               end,
          { AccNW + 1
          , [Op | AccOps]
          }
      end,
      {LogSize0, []},
      Dirty),
  ok = write_log(Log, Ops),
  S#s{ dirty = #{}
     , log_size = LogSize
     }.

-spec do_compaction(s()) -> {ok, s()} | {error, _Reason, s()}.
do_compaction(S0 = #s{name = Name, ets = Ets}) ->
  S1 = #s{log = Old} = handle_flush(S0),
  ok = close_log(Old),
  S = S1#s{log = undefined},
  try
     NewName = log_name(S, ".NEW"),
     OldName = log_name(S, ""),
     {ok, Log} = open_log(NewName, read_write),
     LogSize =
       dump_ets(
         Log,
         0,
         ets:match(Ets, '$1', batch_size())),
     ok = rename_log(NewName, OldName),
     {ok, S#s{log = Log, dirty = #{}, log_size = LogSize}}
  catch
    EC:Err:Stack ->
      logger:error(#{ msg => failed_to_compact_classy_log
                    , EC => Err
                    , stack => Stack
                    , table => Name
                    }),
      {error, Err, S}
  end.

dump_ets(_Log, N, '$end_of_table') ->
  N;
dump_ets(Log, N, {Batch, Cont}) ->
  Recs = lists:map(
           fun([#classy_kv{k = K, v = V}]) ->
               ?w(K, V)
           end,
           Batch),
  ok = write_log(Log, Recs),
  dump_ets(
    Log,
    N + length(Recs),
    ets:match(Cont)).

handle_drop(From, S = #s{ets = Ets, log = Log}) ->
  ets:delete(Ets),
  close_log(Log),
  file:delete(log_name(S, "")),
  file:delete(log_name(S, ".NEW")),
  gen_server:reply(From, ok),
  undefined.

maybe_compact(From, Reply, S0 = #s{badness_threshold = Threshold}) ->
  case log_badness(S0) >= Threshold of
    true ->
      case do_compaction(S0) of
        {ok, S} ->
          {reply, Reply, S};
        {error, Reason, S} ->
          gen_server:reply(From, {error, Reason}),
          {stop, Reason, S}
      end;
    false ->
      {reply, Reply, S0}
  end.

log_badness(#s{ets = ETS, log_size = LogSize}) ->
  NItems = ets:info(ETS, size),
  max(0, LogSize - NItems).

batch_size() ->
  application:get_env(classy, table_batch_size, 100).

-ifndef(CONCUERROR).
rename_log(From, To) ->
  file:rename(From, To).

is_log(Filename) ->
  filelib:is_file(Filename).

open_log(Filename, Mode) ->
  Opts = [ {name, make_ref()}
         , {file, classy_lib:ensure_list(Filename)}
         , {mode, Mode}
         , {format, internal}
         , {type, halt}
         , {size, infinity}
         , {repair, true}
         , {notify, false}
         , {linkto, self()}
         ],
  case disk_log:open(Opts) of
    {ok, Log} ->
      {ok, Log};
    {repaired, Log, _Recovered, _BadBytes} ->
      {ok, Log};
    {error, Reason} ->
      {error, Reason}
  end.

close_log(undefined) ->
  ok;
close_log(Log) ->
  disk_log:close(Log).

write_log(Log, Terms) ->
  disk_log:log_terms(Log, Terms).

read_log_chunk(Log, Cont, Size) ->
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

ensure_open(Pid, Tab) ->
  try
    gen_server:call(Pid, #call_ensure_open{tab = Tab}, infinity)
  catch
    exit:{{classy_table_open_failed, _, _, _} = Reason, {gen_server, call, _}} ->
      {error, Reason};
    exit:{Reason, {gen_server, call, _}} ->
      {error, Reason};
    exit:Reason ->
      {error, {classy_table_open_failed, Tab, Reason}}
  end.

with_log_open(Filename, Mode, S = #s{name = Name, dir = Dir}) ->
  case open_log(Filename, Mode) of
    {ok, Log} ->
      {ok, S#s{log = Log}};
    {error, Reason} ->
      logger:error(
        #{ msg => classy_table_open_failed
         , table => Name
         , dir => Dir
         , file => Filename
         , mode => Mode
         , reason => Reason
         }),
      {stop, {classy_table_open_failed, Name, Filename, Reason}, S}
  end.

log_restore_failure(#s{name = Name, dir = Dir}, Filename, Reason) ->
  logger:error(
    #{ msg => classy_table_restore_failed
     , table => Name
     , dir => Dir
     , file => Filename
     , reason => Reason
     }).

-endif.
