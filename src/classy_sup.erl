%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_sup).

-behavior(supervisor).

%% API:
-export([ start_link/1
        , start_pstore/2
        , start_membership/2
        ]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([ start_link_pstore_sup/1
        , start_link_membership_sup/1
        ]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(top, {rt :: module()}).
-record(pstore_sup, {rt :: module()}).
-record(membership_sup, {rt :: module()}).

-define(SUP, ?MODULE).
-define(PSTORE_SUP, ?MODULE).
-define(MEM_SUP, ?MODULE).

%%================================================================================
%% API functions
%%================================================================================


-spec start_link(module()) -> supervisor:startlink_ret().
start_link(RT) ->
  supervisor:start_link({local, ?SUP}, ?MODULE, #top{rt = RT}).

-spec start_pstore(classy_pstore:tab(), classy_pstore:options()) -> {ok, pid()} | {error, _}.
start_pstore(Tab, Options) ->
  supervisor:start_child(?PSTORE_SUP, [Tab, Options]).

-spec start_membership(classy:cluster_id(), classy:site()) -> {ok, pid()} | {error, _}.
start_membership(Cluster, Site) ->
  supervisor:start_child(?MEM_SUP, [Cluster, Site]).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_pstore_sup(module()) -> supervisor:startlink_ret().
start_link_pstore_sup(RT) ->
  supervisor:start_link({local, ?PSTORE_SUP}, ?MODULE, #pstore_sup{rt = RT}).

-spec start_link_membership_sup(module()) -> supervisor:startlink_ret().
start_link_membership_sup(RT) ->
  supervisor:start_link({local, ?MEM_SUP}, ?MODULE, #membership_sup{rt = RT}).

%%================================================================================
%% behavior callbacks
%%================================================================================

init(#top{rt = RT}) ->
  Children = [ sup_spec(#{id => pstore_sup, start => {?MODULE, start_link_pstore_sup, [RT]}})
             , sup_spec(#{id => membership_sup, start => {?MODULE, start_link_membership_sup, [RT]}})
             ],
  SupFlags = #{ strategy      => one_for_all
              , intensity     => 10
              , period        => 10
              , auto_shutdown => never
              },
  {ok, {SupFlags, Children}};
init(#pstore_sup{rt = RT}) ->
  Children = #{ id       => worker
              , start    => {classy_pstore, start_link, [RT]}
              , shutdown => 5_000
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
