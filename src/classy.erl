%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy).

%% API:
-export([ join_node/2
        , kick_site/2
        , kick_node/2
        , sites/0
        , nodes/1
        , quorum/1
        ]).

-export([ on_node_init/2
        , on_create_cluster/2
        , on_create_site/2
        , on_site_status_change/2
        , on_membership_change/2
        , pre_join/2
        , post_join/2
        , pre_kick/2
        , post_kick/2
        , run_level/2
        ]).

-export_type([ cluster_id/0
             , site/0

             , run_level/0
             , site_status_hook/0
             , membership_change_hook/0
             ]).

-include("classy_internal.hrl").
-compile({no_auto_import, [nodes/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

-type cluster_id() :: binary().

-type site() :: binary().

-type site_status_hook() :: fun((cluster_id(), _Local :: site(), _Up :: boolean()) -> _).

-type membership_change_hook() :: fun((cluster_id(), _Local :: site(), _Remote :: site(), _IsMember :: boolean()) -> _).

-type join_intent() :: join
                     | _.

-type kick_intent() :: join   %% Intent set by system when site leaves the cluster to join another one
                     | kicked %% Intent set by system when site is kicked by the third party
                     | _.

-type run_level() :: stopped | single | cluster.

%%================================================================================
%% API functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Cluster management
%%--------------------------------------------------------------------------------

-spec join_node(node(), join_intent()) -> ok | {error, _}.
join_node(Node, Intent) ->
  classy_node:join_node(Node, Intent).

-spec kick_site(site(), kick_intent()) -> ok | {error, _}.
kick_site(Site, Intent) ->
  classy_node:kick_site(Site, Intent).

-spec kick_node(node(), kick_intent()) -> ok | {error, _}.
kick_node(Node, Intent) ->
  case {classy_node:the_cluster(), classy_node:the_site()} of
    {{ok, Cluster}, {ok, Local}} ->
      case classy_membership:site_of_node(Cluster, Local) of
        #{Node := Site} ->
          kick_site(Site, Intent);
        #{} ->
          {error, target_not_in_cluster}
      end;
    _ ->
      {error, local_not_in_cluster}
  end.

-spec sites() -> [site()].
sites() ->
  maybe
    {ok, Cluster} ?= classy_node:the_cluster(),
    {ok, Local} ?= classy_node:the_site(),
    ?tp_span(classy_get_members, #{},
    classy_membership:members(Cluster, Local))
  else
    _ ->
      []
  end.

-spec nodes(all | running | stopped) -> [{site(), node()}].
nodes(running) ->
  maybe
    {ok, Cluster} ?= classy_node:the_cluster(),
    {ok, Local} ?= classy_node:the_site(),
    %% FIXME: optimize
    Members = classy_membership:members(Cluster, Local),
    Sites = classy_membership:site_of_node(Cluster, Local),
    [I || I <- [node() | erlang:nodes()],
          case Sites of
            #{I := Site} -> lists:member(Site, Members);
            _ -> false
          end]
  else _ ->
      []
  end;
nodes(stopped) ->
  nodes(all) -- nodes(running);
nodes(all) ->
  maybe
    {ok, Cluster} ?= classy_node:the_cluster(),
    {ok, Local} ?= classy_node:the_site(),
    Sites = classy_membership:members(Cluster, Local),
    Nodes = classy_membership:node_of_site(Cluster, Local),
    lists:flatmap(
      fun(Site) ->
          case Nodes of
            #{Site := Node} -> [Node];
            #{} -> []
          end
      end,
      Sites)
  else
    _ ->
      []
  end.

%%--------------------------------------------------------------------------------
%% Misc.
%%--------------------------------------------------------------------------------

%% @doc Calculate the number of nodes required for the quorum:
%%
%% - `Integer': any integer value
%% - `config': Return value of `classy.quorum' application environment variable
%% - `running': Quorum among the running sites.
%%    Returned value is greater or equal to `quorum(config)'
-spec quorum(config | running | non_neg_integer()) -> pos_integer().
quorum(N) when is_integer(N), N >= 0 ->
  trunc(N / 2) + 1;
quorum(config) ->
  max(1, application:get_env(classy, quorum, 1));
quorum(running) ->
  max(
    quorum(length(nodes(running))),
    quorum(config)).

%%--------------------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------------------

%% Note: business release can install hooks by setting
%% `classy:setup_hooks' application environment variable to a tuple
%% `{Module, Function, Args}'. This MFA can contain calls to various
%% `classy:on_...' functions.

%% @doc Register a hook that is executed when the node (not the site)
%% starts. It is called before `the_site' and `the_cluster' are
%% initialized and can be used to overreride the default cluster and
%% site initialization logic.
-spec on_node_init(fun(() -> _), classy_hook:prio()) -> classy_hook:hook().
on_node_init(Hook, Prio) ->
  classy_hook:insert(?on_node_init, Hook, Prio).

%% @doc This callback is called once per cluster by the site that
%% originally creates the cluster.
-spec on_create_cluster(fun((cluster_id()) -> _), classy_hook:prio()) -> classy_hook:hook().
on_create_cluster(Hook, Prio) ->
  classy_hook:insert(?on_create_cluster, Hook, Prio).

%% @doc This callback is called once per site.
-spec on_create_site(fun((site()) -> _), classy_hook:prio()) -> classy_hook:hook().
on_create_site(Hook, Prio) ->
  classy_hook:insert(?on_create_site, Hook, Prio).

%% @doc Register a hook that is executed when a site changes
%% status from up to down and vice versa.
-spec on_site_status_change(site_status_hook(), classy_hook:prio()) -> classy_hook:hook().
on_site_status_change(Hook, Prio) ->
  classy_hook:insert(?on_site_status_change, Hook, Prio).

%% @doc Register a hook that is executed a site joins or leaves a cluster.
-spec on_membership_change(membership_change_hook(), classy_hook:prio()) -> classy_hook:hook().
on_membership_change(Hook, Prio) ->
  classy_hook:insert(?on_membership_change, Hook, Prio).

%% @doc Register a hook that is executed before the local node joins a
%% remote site and/or cluster. WARNING: this hook should not have side
%% effects. It should only check if it is ok to join.
-spec pre_join(
        fun((classy:cluster_id(), Remote, node(), join_intent()) -> ok | {error, _}),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Remote :: site().
pre_join(Hook, Prio) ->
  classy_hook:insert(?on_pre_join, Hook, Prio).

%% @doc Register a hook that is executed after a local site joins a
%% cluster.
-spec post_join(
        fun((classy:cluster_id(), Local) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Local :: classy:site().
post_join(Hook, Prio) ->
  classy_hook:insert(?on_post_join, Hook, Prio).

%% @doc Register a hook that verifies whether or not a site can be
%% kicked from the cluster. This hook runs on the node that initiates
%% the kick.
%%
%% WARNING: this hook cannot have side effects.
-spec pre_kick(
        fun((cluster_id(), Remote, kick_intent()) -> ok | {error, _}),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Remote :: site().
pre_kick(Hook, Prio) ->
  classy_hook:insert(?on_pre_kick, Hook, Prio).

%% @doc Register a hook that is executed after a local site leaves a
%% cluster. This hook can perform destructive actions associated with
%% cleanup.
-spec post_kick(
        fun((cluster_id(), Local, kick_intent()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Local :: site().
post_kick(Hook, Prio) ->
  classy_hook:insert(?on_post_kick, Hook, Prio).


%% @doc Register a hook that is executed on change of the run level of
%% the local site.
-spec run_level(
        fun((run_level(), run_level()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook().
run_level(Hook, Prio) ->
  classy_hook:insert(?on_change_run_level, Hook, Prio).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
