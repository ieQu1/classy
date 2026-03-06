%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy).

%% API:
-export([ on_node_init/2
        , on_site_status_change/2
        , on_membership_change/2
        , pre_join/2
        , post_join/2
        , pre_leave/2
        , post_leave/2
        ]).

-export_type([ cluster_id/0
             , site/0

             , site_status_hook/0
             , membership_change_hook/0
             , pre_cluster_hook/0
             , post_cluster_hook/0
             ]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type cluster_id() :: term().

-type site() :: term().

-type site_status_hook() :: fun((cluster_id(), _Local :: site(), _Up :: boolean()) -> _).

-type membership_change_hook() :: fun((cluster_id(), _Local :: site(), _Remote :: site(), _IsMember :: boolean()) -> _).

-type pre_cluster_hook() :: fun((cluster_id(), _Local :: site()) -> ok | {error, string()}).

-type post_cluster_hook() :: fun((cluster_id(), _Local :: site()) -> _).

%%================================================================================
%% API functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Cluster management
%%--------------------------------------------------------------------------------

%%--------------------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------------------

%% Note: business release can install hooks by setting
%% `classy:setup_hooks' application environment variable to a tuple
%% `{Module, Function, Args}'. This MFA can contain calls to various
%% `classy:on_...' functions.

%% @doc Register a hook that is executed when the node (not the site)
%% starts. It is called before `the_site' and `the_cluster' are
%% initialized.
-spec on_node_init(fun(() -> ok), classy_hook:prio()) -> ok.
on_node_init(Hook, Prio) ->
  classy_hook:insert(?on_node_init, Hook, Prio).

%% @doc Register a hook that is executed when a site changes
%% status from up to down and vice versa.
-spec on_site_status_change(site_status_hook(), classy_hook:prio()) -> ok.
on_site_status_change(Hook, Prio) ->
  classy_hook:insert(?on_site_status_change, Hook, Prio).

%% @doc Register a hook that is executed a site joins or leaves a cluster.
-spec on_membership_change(membership_change_hook(), classy_hook:prio()) -> ok.
on_membership_change(Hook, Prio) ->
  classy_hook:insert(?on_membership_change, Hook, Prio).

%% @doc Register a hook that is executed before the local node joins a
%% remote site and/or cluster. WARNING: this hook should not have side
%% effects. It should only check if it is ok to join.
-spec pre_join(
        fun((classy:cluster_id(), Remote) -> ok | {error, _}),
        classy_hook:prio()
       ) -> ok
   when Remote :: classy:site().
pre_join(Hook, Prio) ->
  classy_hook:insert(?on_pre_join, Hook, Prio).

%% @doc Register a hook that is executed after a local site joins a
%% cluster.
-spec post_join(
        fun((classy:cluster_id(), Remote) -> _),
        classy_hook:prio()
       ) -> ok
   when Remote :: classy:site().
post_join(Hook, Prio) ->
  classy_hook:insert(?on_post_join, Hook, Prio).

%% @doc Register a hook that is executed before a local site leaves a
%% cluster. Note: this hook should not have side effects. It's meant
%% to check if it is ok to leave.
-spec pre_leave(pre_cluster_hook(), classy_hook:prio()) -> ok.
pre_leave(Hook, Prio) ->
  classy_hook:insert(?on_pre_leave, Hook, Prio).

%% @doc Register a hook that is executed after a local site leaves a
%% cluster. This hook can perform destructive actions associated with
%% cleanup.
-spec post_leave(post_cluster_hook(), classy_hook:prio()) -> ok.
post_leave(Hook, Prio) ->
  classy_hook:insert(?on_post_leave, Hook, Prio).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
