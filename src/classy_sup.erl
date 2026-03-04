%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_sup).

-behavior(supervisor).

%% API:
-export([ start_link/1
        , stop/1
        , start_table/2
        , start_membership/2
        ]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([ start_link_table_sup/1
        , start_link_membership_sup/1
        ]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(top, {rt :: module()}).
-record(table_sup, {rt :: module()}).
-record(membership_sup, {rt :: module()}).

-define(SUP, ?MODULE).
-define(TABLE_SUP, classy_table_sup).
-define(MEMBERSHIP_SUP, classy_membership_sup).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(module()) -> supervisor:startlink_ret().
start_link(RT) ->
  supervisor:start_link({local, ?SUP}, ?MODULE, #top{rt = RT}).

-spec stop(timeout()) -> ok.
stop(Timeout) ->
  case whereis(?SUP) of
    Pid when is_pid(Pid) ->
      unlink(Pid),
      MRef = monitor(process, Pid),
      exit(Pid, shutdown),
      receive
        {'DOWN', MRef, process, _, _} ->
          ok
      after Timeout ->
          {error, timeout}
      end;
    undefined ->
      ok
  end.

-spec start_table(classy_table:tab(), classy_table:options()) -> {ok, pid()} | {error, _}.
start_table(Tab, Options) ->
  supervisor:start_child(?TABLE_SUP, [Tab, Options]).

-spec start_membership(classy:cluster_id(), classy:site()) -> {ok, pid()} | {error, _}.
start_membership(Cluster, Site) ->
  supervisor:start_child(?MEMBERSHIP_SUP, [Cluster, Site]).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_table_sup(module()) -> supervisor:startlink_ret().
start_link_table_sup(RT) ->
  supervisor:start_link({local, ?TABLE_SUP}, ?MODULE, #table_sup{rt = RT}).

-spec start_link_membership_sup(module()) -> supervisor:startlink_ret().
start_link_membership_sup(RT) ->
  supervisor:start_link({local, ?MEMBERSHIP_SUP}, ?MODULE, #membership_sup{rt = RT}).

%%================================================================================
%% behavior callbacks
%%================================================================================

init(#top{rt = RT}) ->
  Children = [ sup_spec(#{id => ?TABLE_SUP, start => {?MODULE, start_link_table_sup, [RT]}})
             , sup_spec(#{id => ?MEMBERSHIP_SUP, start => {?MODULE, start_link_membership_sup, [RT]}})
             ],
  SupFlags = #{ strategy      => one_for_all
              , intensity     => 10
              , period        => 10
              , auto_shutdown => never
              },
  {ok, {SupFlags, Children}};
init(#table_sup{rt = RT}) ->
  Children = #{ id       => worker
              , start    => {classy_table, start_link, [RT]}
              , shutdown => infinity
              , type     => worker
              , restart  => temporary
              },
  SupFlags = #{ strategy      => simple_one_for_one
              , intensity     => 10
              , period        => 10
              , auto_shutdown => never
              },
  {ok, {SupFlags, [Children]}};
init(#membership_sup{rt = RT}) ->
  Children = #{ id       => worker
              , start    => {classy_membership, start_link, [RT]}
              , shutdown => 5_000
              , type     => worker
              , restart  => temporary
              },
  SupFlags = #{ strategy      => simple_one_for_one
              , intensity     => 10
              , period        => 10
              , auto_shutdown => never
              },
  {ok, {SupFlags, [Children]}}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec sup_spec(map()) -> supervisor:child_spec().
sup_spec(M) ->
  maps:merge(
    #{ shutdown    => infinity
     , restart     => permanent
     , type        => supervisor
     , significant => false
     },
    M).
