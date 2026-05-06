%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module contains utilities for constructing unique IDs
%% using various algorithms.
-module(classy_uid).

-behavior(gen_server).

%% API:
-export([ new_seq_tuple/0
        , new_cseq_tuple/0
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/0]).

-export_type([seqtuple/0, cseqtuple/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(pterm_uid_gen, classy_uid_gen).

-type pterm_uid_gen() :: #{ site               := classy:site()
                          , n_restarts         := non_neg_integer()
                          , volatile_seqno_ctr := atomics:atomics_ref()
                          }.

-type seqtuple() :: {non_neg_integer(), pos_integer()}.

-type cseqtuple() :: {classy:site(), non_neg_integer(), pos_integer()}.

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

%% @private
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Return a "seqtuple" ID that is unique within the site, but is
%% NOT unique in a cluster.
%%
%% SeqTuple consists of a number of node restarts followed by a
%% node-global counter that resets on each node restart. Creating
%% seq_tuples is a relatively cheap operation, and most of the time
%% they form a straightforward arithmetic progression.
-spec new_seq_tuple() -> seqtuple().
new_seq_tuple() ->
  #{n_restarts := NRestarts, volatile_seqno_ctr := Ctr} = get_pterm(),
  Seq = atomics:add_get(Ctr, 1, 1),
  {NRestarts, Seq}.

%% @doc Return a "cseqtuple" ID. It is a seqtuple with site id.
-spec new_cseq_tuple() -> cseqtuple().
new_cseq_tuple() ->
  #{n_restarts := NRestarts, volatile_seqno_ctr := Ctr, site := Site} = get_pterm(),
  Seq = atomics:add_get(Ctr, 1, 1),
  {Site, NRestarts, Seq}.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {}).

init(_) ->
  process_flag(trap_exit, true),
  {ok, NRestarts} = classy_node:n_restarts(),
  {ok, Site} = classy_node:the_site(),
  set_pterm(#{ site               => Site
             , n_restarts         => NRestarts
             , volatile_seqno_ctr => atomics:new(1, [{signed, false}])
             }),
  S = #s{},
  {ok, S}.

handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
  {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, _S) ->
  persistent_term:erase(?pterm_uid_gen),
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

-spec set_pterm(pterm_uid_gen()) -> ok.
set_pterm(PT) ->
  persistent_term:put(?pterm_uid_gen, PT).

-spec get_pterm() -> pterm_uid_gen().
get_pterm() ->
  persistent_term:get(?pterm_uid_gen).
