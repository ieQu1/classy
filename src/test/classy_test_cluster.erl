%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A process that manages test clusters.
%%
%% This module is not part of the classy runtime,
%% but it's useful for testing the business apps based on classy.
%%
%% Cluster configuration:
%% <itemize>
%% <li>`peer': <a href="https://www.erlang.org/doc/apps/stdlib/peer.html#t:start_options/0">peer</a> start options.
%% Optional.</li>
%% <li>`fixtures': list of {@link classy_test_fixture . fixtures}.
%% Mandatory.</li>
%% </itemize>
-module(classy_test_cluster).

-behavior(supervisor).

%% API:
-export([start_link/1, stop/1, ensure_site/2, merge_conf/2]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([ start_link_site_sup/2
        , start_link_cleanup/2
        , cluster_cleanup_entrypoint/3
        , exit_success/1
        ]).

-export_type([conf/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(top, classy_test_cluster).
-define(sites, classy_test_cluster_sites).

-type conf() ::
        #{ peer => map()
         , fixtures => [classy_test_fixture:t()]
         }.

-define(pt_success, classy_test_cluster_success).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Start the cluster.
-spec start_link(classy_test_site:conf()) -> supervisor:startlink_ret().
start_link(Conf = #{fixtures := Fixtures}) when is_list(Fixtures) ->
  {ok, _} = application:ensure_all_started(gproc),
  supervisor:start_link({local, ?top}, ?MODULE, {top, Conf}).

%% @doc Create a site, if not created yet.
%% Note: this function doesn't start the site.
-spec ensure_site(classy:site(), classy_test_site:conf()) -> ok | {error, _}.
ensure_site(Site, Conf) ->
  case supervisor:start_child(?sites, [Site, Conf]) of
    {ok, _} ->
      ok;
    {error, {already_started, _}} ->
      ok;
    Err ->
      Err
  end.

%% @doc Stop the cluster.
-spec stop(_Reason) -> ok.
stop(Reason) ->
  persistent_term:put(?pt_success, classy_lib:is_normal_exit(Reason)),
  classy_lib:sync_stop_proc(?top, shutdown, infinity).

%% @doc Merge cluster configuration.
-spec merge_conf(conf(), conf()) -> conf().
merge_conf(C1, C2) ->
  maps:merge_with(
    fun(fixtures, A, B) ->
        A ++ B;
       (peer, A, B) ->
        maps:merge(A, B)
    end,
    C1,
    C2).

%%================================================================================
%% Internal exports
%%================================================================================

%% @private
exit_success(Reason) ->
  classy_lib:is_normal_exit(Reason) andalso persistent_term:get(?pt_success, true).

%% @private
start_link_site_sup(Conf, FixtureState) ->
  supervisor:start_link({local, ?sites}, ?MODULE, {sites, Conf, FixtureState}).

%% @private
start_link_cleanup(Fixtures, FixtureState) ->
  proc_lib:start_link(?MODULE, cluster_cleanup_entrypoint, [self(), Fixtures, FixtureState]).

%% @private
cluster_cleanup_entrypoint(Parent, Fixtures, FixtureState) ->
  process_flag(trap_exit, true),
  proc_lib:init_ack(Parent, {ok, self()}),
  receive
    {'EXIT', _, Reason} ->
      Success = exit_success(Reason),
      persistent_term:erase(?pt_success),
      classy_test_fixture:cleanup_per_cluster(
        Fixtures,
        Success,
        FixtureState),
      exit(shutdown)
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%% @private
init({top, Conf}) ->
  #{fixtures := Fixtures} = Conf,
  case classy_test_fixture:init_per_cluster(Fixtures) of
    {ok, FixtureState} ->
      SupFlags = #{ strategy  => one_for_all
                  , intensity => 1
                  , period    => 1
                  },
      Children = [ #{ id => cleanup
                    , type => worker
                    , restart => permanent
                    , start => {?MODULE, start_link_cleanup, [Fixtures, FixtureState]}
                    , shutdown => 5_000
                    }
                 , #{ id => sites
                    , type => supervisor
                    , restart => permanent
                    , start => {?MODULE, start_link_site_sup, [Conf, FixtureState]}
                    , shutdown => infinity
                    }
                 ],
      {ok, {SupFlags, Children}};
    {error, Err}->
      error({cluster_initialization_failed, Err})
  end;
init({sites, CommonConf, FixtureState}) ->
  SupFlags = #{ strategy      => simple_one_for_one
              , intensity     => 10
              , period        => 1
              , auto_shutdown => never
              },
  Children = #{ id       => peer
              , type     => worker
              , start    => {classy_test_site, start_link, [CommonConf, FixtureState]}
              , shutdown => 15_000
              , restart  => permanent
              },
  {ok, {SupFlags, [Children]}}.

%%================================================================================
%% Internal functions
%%================================================================================
