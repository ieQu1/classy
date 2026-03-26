%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @private
-module(classy_sup).

-behavior(supervisor).

%% API:
-export([ start_link/0
        , stop/1
        , start_table/2
        , ensure_membership/2
        ]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([ start_link_table_sup/0
        , start_link_membership_sup/0
        ]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(top, {}).
-record(table_sup, {}).
-record(membership_sup, {}).

-define(SUP, ?MODULE).
-define(TABLE_SUP, classy_table_sup).
-define(MEMBERSHIP_SUP, classy_membership_sup).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
  supervisor:start_link({local, ?SUP}, ?MODULE, #top{}).

-spec stop(timeout()) -> ok.
stop(Timeout) ->
  classy_lib:sync_stop_proc(?SUP, shutdown, Timeout).

-spec start_table(classy_table:tab(), classy_table:options()) -> {ok, pid()} | {error, _}.
start_table(Tab, Options) ->
  supervisor:start_child(?TABLE_SUP, [Tab, Options]).

-spec ensure_membership(classy:cluster_id(), classy:site()) -> {ok, pid()} | {error, _}.
ensure_membership(Cluster, Site) ->
  case supervisor:start_child(?MEMBERSHIP_SUP, [Cluster, Site]) of
    {ok, _} = Ok ->
      Ok;
    {error, {already_started, Pid}} ->
      {ok, Pid};
    Err ->
      Err
  end.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_table_sup() -> supervisor:startlink_ret().
start_link_table_sup() ->
  supervisor:start_link({local, ?TABLE_SUP}, ?MODULE, #table_sup{}).

-spec start_link_membership_sup() -> supervisor:startlink_ret().
start_link_membership_sup() ->
  supervisor:start_link({local, ?MEMBERSHIP_SUP}, ?MODULE, #membership_sup{}).

%%================================================================================
%% behavior callbacks
%%================================================================================

init(#top{}) ->
  case classy_hook:init() of
    ok ->
      Node = #{ id       => node
              , start    => {classy_node, start_link, []}
              , shutdown => 5_000
              , restart  => permanent
              , type     => worker
              },
      Children = [ sup_spec(#{id => ?TABLE_SUP, start => {?MODULE, start_link_table_sup, []}})
                 , sup_spec(#{id => ?MEMBERSHIP_SUP, start => {?MODULE, start_link_membership_sup, []}})
                 , Node
                 ],
      SupFlags = #{ strategy      => rest_for_one
                  , intensity     => 10
                  , period        => 10
                  , auto_shutdown => never
                  },
      {ok, {SupFlags, Children}};
    {error, Reason} ->
      {stop, Reason}
  end;
init(#table_sup{}) ->
  Children = #{ id       => worker
              , start    => {classy_table, start_link, []}
              , shutdown => infinity
              , type     => worker
              , restart  => permanent
              },
  SupFlags = #{ strategy      => simple_one_for_one
              , intensity     => 10
              , period        => 10
              , auto_shutdown => never
              },
  {ok, {SupFlags, [Children]}};
init(#membership_sup{}) ->
  Children = #{ id       => worker
              , start    => {classy_membership, start_link, []}
              , shutdown => 5_000
              , type     => worker
              , restart  => permanent
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
