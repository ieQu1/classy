%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy).

%% API:
-export([ on_site_status_change/2
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

%% @doc Register a hook that is executed when a site changes
%% status from up to down and vice versa.
-spec on_site_status_change(site_status_hook(), classy_hook:prio()) -> ok.
on_site_status_change(Hook, Prio) ->
  classy_hook:insert(site_status, Hook, Prio).

%% @doc Register a hook that is executed a site joins or leaves a cluster.
-spec on_membership_change(membership_change_hook(), classy_hook:prio()) -> ok.
on_membership_change(Hook, Prio) ->
  classy_hook:insert(membership_change, Hook, Prio).

%% @doc Register a hook that is executed before a local site joins a
%% cluster. It can abort this operation.
-spec pre_join(pre_cluster_hook(), classy_hook:prio()) -> ok.
pre_join(Hook, Prio) ->
  classy_hook:insert(pre_join, Hook, Prio).

%% @doc Register a hook that is executed after a local site joins a
%% cluster.
-spec post_join(post_cluster_hook(), classy_hook:prio()) -> ok.
post_join(Hook, Prio) ->
  classy_hook:insert(post_join, Hook, Prio).

%% @doc Register a hook that is executed before a local site leaves
%% a cluster. It can abort this operation.
-spec pre_leave(pre_cluster_hook(), classy_hook:prio()) -> ok.
pre_leave(Hook, Prio) ->
  classy_hook:insert(pre_leave, Hook, Prio).

%% @doc Register a hook that is executed after a local site leaves a
%% cluster.
-spec post_leave(post_cluster_hook(), classy_hook:prio()) -> ok.
post_leave(Hook, Prio) ->
  classy_hook:insert(post_leave, Hook, Prio).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
