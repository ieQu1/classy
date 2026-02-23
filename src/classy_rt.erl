%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_rt).

%% API:
-export([init/3, terminate/2, get_membership_pid/3, pset/4, pdel/3, plist/3, pflush/2, time_s/1]).

-export_type([cbs/0]).

-include("classy_rt.hrl").

%%================================================================================
%% Behavior definition:
%%================================================================================

-doc """
State of the callback module.
It is opaque to us.
""".
-type cbs() :: term().

-callback classy_init(classy:cluster_id(), classy:site()) -> {ok, cbs()}.

-callback classy_terminate(cbs()) -> _.

-doc """
Get PID of a potentially remote `classy_membership` server.
""".
-callback classy_get_membership_pid(classy:cluster_id(), classy:site()) -> pid() | undefined.

-doc """
Store a key-value pair persistently.

Note: the runtime may buffer `classy_p(set/del)` operations.
""".
-callback classy_pset(cbs(), {?cl_last, classy:site()}, classy_membership:op()) -> ok;
                     (cbs(), ?cl_clock, classy_membership:clock()) -> ok;
                     (cbs(), {?cl_acked_in | ?cl_acked_out, classy:site()}, classy_membership:clock()) -> ok.

-doc """
Delete a key-value pair persistently.
""".
-callback classy_pdel(cbs(), {?cl_last, classy:site()}) -> ok;
                     (cbs(), ?cl_clock) -> ok;
                     (cbs(), {?cl_acked_in | ?cl_acked_out, classy:site()}) -> ok.

-doc """
List persistent values.
""".
-callback classy_plist(cbs(), ?cl_last) -> [{classy:site(), classy_membership:op()}];
                      (cbs(), ?cl_clock) -> [classy_membership:clock()];
                      (cbs(), ?cl_acked_in | ?cl_acked_out) -> [{classy:site(), classy_membership:clock()}].

-doc """
Flush the persistence buffers.
""".
-callback classy_psync(cbs()) -> ok.

-doc """
Get local unix time (in seconds).
""".
-callback classy_time_s() -> non_neg_integer().

%%================================================================================
%% API functions
%%================================================================================

-spec init(module(), classy:cluster_id(), classy:site()) -> {ok, cbs()}.
init(Mod, Cluster, Site) ->
  Mod:classy_init(Cluster, Site).

-spec terminate(module(), cbs()) -> ok.
terminate(Mod, CBS) ->
  Mod:classy_terminate(CBS),
  ok.

-spec get_membership_pid(module(), classy:cluster_id(), classy:site()) -> pid() | undefined.
get_membership_pid(Mod, Cluster, Site) ->
  Mod:classy_get_membership_pid(Cluster, Site).

-spec pset(module(), cbs(), {?cl_last, classy:site()}, classy_membership:op()) -> ok;
          (module(), cbs(), ?cl_clock, classy_membership:clock()) -> ok;
          (module(), cbs(), {?cl_acked_in | ?cl_acked_out, classy:site()}, classy_membership:clock()) -> ok.
pset(Mod, CBS, K, V) ->
  Mod:classy_pset(CBS, K, V).

-spec pdel(module(), cbs(), {?cl_last, classy:site()}) -> ok;
          (module(), cbs(), ?cl_clock) -> ok;
          (module(), cbs(), {?cl_acked_in | ?cl_acked_out, classy:site()}) -> ok.
pdel(Mod, CBS, K) ->
  Mod:classy_pdel(CBS, K).

-spec plist(module(), cbs(), ?cl_last) -> [{classy:site(), classy_membership:op()}];
           (module(), cbs(), ?cl_clock) -> [classy_membership:clock()];
           (module(), cbs(), ?cl_acked_in | ?cl_acked_out) -> [{classy:site(), classy_membership:clock()}].
plist(Mod, CBS, Kind) ->
  Mod:classy_plist(CBS, Kind).

-spec pflush(module(), cbs()) -> ok.
pflush(Mod, CBS) ->
  Mod:classy_psync(CBS).

-spec time_s(module()) -> non_neg_integer().
time_s(Mod) ->
  Mod:classy_time_s().
