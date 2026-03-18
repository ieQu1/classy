%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_fuzzer).

-compile(export_all).
-compile(nowarn_export_all).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-define(workdir, "classy_fuzzer_workdir").
-define(sites, [<<"S1">>, <<"S2">>, <<"S3">>, <<"S4">>, <<"S5">>]).
-define(node_name_suffix, [<<"n1">>, <<"n2">>]).

-type site_state() ::
        #{ peer_spec  := classy_ct:start_spec()
         , cluster    := classy:cluster_id()
         , running    := boolean()
         }.

-type s() ::
        #{ n_sites := pos_integer()
         , quorum := pos_integer()
         , sites := #{classy:site() => site_state()}
         , run_state := #{classy:site() => classy_ct:run_info()}
         }.

-type tc_conf() :: _.

%%--------------------------------------------------------------------
%% Proper generators
%%--------------------------------------------------------------------

start_spec_(Cluster, Site, NSites, Quorum) ->
  ?LET(Suffix,
       oneof(?node_name_suffix),
       begin
         NodeName = node_name(Site, Suffix),
         {ok, CWD} = file:get_cwd(),
         WorkDir = binary_to_list(filename:join([CWD, ?workdir, Site])),
         exactly(
           { Site
           , #{ name => NodeName
              , env => [ {classy, table_dir, WorkDir}
                       , {classy, n_sites, NSites}
                       , {classy, quorum, Quorum}
                       , {classy, setup_hooks, {?MODULE, setup_hooks, [Cluster, Site]}}
                       , {classy, sync_timeout, 1}
                       ]
              , cover => true
              , code_paths => classy_ct:master_code_paths()
              , workdir => WorkDir
              }
           })
       end).

foo() ->
  #{foo => integer(), bar => float()}.

init_cluster_(_S) ->
  N = length(?sites) div 2,
  ?LET(
     {NSites, Quorum, Foo},
     {range(1, N), range(1, N), foo()},
     ?LET(
        SiteSpecs,
        fixed_list([start_spec_(Site, Site, NSites, Quorum) || Site <- ?sites]),
        begin
          Sites = maps:from_list(
                    [{Site,
                      #{ cluster   => Site
                       , peer_spec => Spec
                       , running   => true
                       }}
                     || {Site, Spec} <- SiteSpecs]),
          {call, ?MODULE, init_cluster,
           [#{ n_sites   => NSites
             , quorum    => Quorum
             , sites     => Sites
             , run_state => #{}
             }]}
        end)).

running_nodes(#{sites := Sites}) ->
  maps:fold(
    fun(Site, #{peer_spec := Spec, running := Running}, Acc) ->
        case Running of
          true  -> [{Site, classy_ct:node_name(Spec)} | Acc];
          false -> Acc
        end
    end,
    [],
    Sites).

site_command_(Site, S = #{run_state := RS, sites := Sites, n_sites := NSites, quorum := Quorum}) ->
  #{Site := #{cluster := Cluster, peer_spec := PeerSpec, running := IsRunning}} = Sites,
  case IsRunning of
    true ->
      frequency(
        [ {1, {call, ?MODULE, stop_site, [Site, S]}}
        ] ++
        [ {2, {call, ?MODULE, join_node, [Site, JoinToSite, JoinToNode, S]}}
          || {JoinToSite, JoinToNode} <- running_nodes(S)
        ]);
    false ->
      ?LET(Spec,
           start_spec_(Cluster, Site, NSites, Quorum),
           {call, ?MODULE, start_site, [Site, S]})
  end.

%%--------------------------------------------------------------------
%% Operations
%%--------------------------------------------------------------------

init_cluster(#{sites := Sites}) ->
  maps:fold(
    fun(Site, #{peer_spec := PeerSpec}, Acc) ->
        start_site(Site, PeerSpec, Acc)
    end,
    #{},
    Sites).

stop_site(Site, S) ->
  #{run_state := RS, sites := Sites} = S,
  #{Site := #{peer_spec := PeerSpec}} = Sites,
  ok = classy_ct:stop_peer(PeerSpec),
  maps:remove(Site, RS).

start_site(Site, S) ->
  #{run_state := RS, sites := Sites} = S,
  #{Site := #{peer_spec := PeerSpec}} = Sites,
  start_site(Site, PeerSpec, RS).

start_site(Site, PeerSpec, RunState) ->
  RunState#{Site => classy_ct:start_peer(classy, PeerSpec)}.

join_node(Site, _JoinToSite, JoinToNode, #{run_state := RS}) ->
  #{Site := R} = RS,
  Intent = join,
  classy_ct:rpc(R, classy, join_node, [JoinToNode, Intent]).

%%--------------------------------------------------------------------
%% Misc. API
%%--------------------------------------------------------------------

format_cmds(Cmds) ->
  lists:map(
    fun({call, ?MODULE, init_cluster, [MS]}) ->
        io_lib:format(" *** init(~p)~n", [MS]);
       ({call, ?MODULE, init_site, [Site, Cluster, Spec]}) ->
        #{name := Name} = Spec,
        io_lib:format(" *** init_site(~s, ~s, ~s)~n", [Site, Cluster, Name]);
       ({init, {init, Cfg}}) ->
        io_lib:format(" *** init model: ~p~n", [Cfg]);
       ({set, _, _}) ->
        [];
       (Other) ->
        io_lib:format(" *** other(~p)~n", [Other])
    end,
    Cmds).

cleanup() ->
  classy_ct:cleanup(t_fuzz),
  file:del_dir_r(?workdir),
  ok.

setup_hooks(Cluster, Site) ->
  classy:on_node_init(
    fun() ->
        classy_node:maybe_init_the_site(Cluster, Site)
    end,
    0).

%%--------------------------------------------------------------------
%% Trace properties
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Statem callbacks
%%--------------------------------------------------------------------


command(S = {init, _}) ->
  init_cluster_(S);
command(S = #{sites := Sites}) ->
  ?LET(
     Site,
     oneof(?sites),
     site_command_(Site, S)).

-spec initial_state() -> s().
initial_state() ->
  initial_state(#{}).

-spec initial_state(tc_conf()) -> s().
initial_state(Conf) ->
  {init, Conf}.

%% Initial connection:
next_state(_, Ret, {call, ?MODULE, init_cluster, [Spec]}) ->
  Spec#{run_state => Ret};
next_state(S = #{run_state := _}, Ret, {call, ?MODULE, stop_site, [Site, _]}) ->
  set_running(Site, false, S#{run_state := Ret});
next_state(S = #{run_state := _}, Ret, {call, ?MODULE, start_site, [Site, _]}) ->
  set_running(Site, true, S#{run_state := Ret});
next_state(S = #{sites := Sites}, _Ret, {call, ?MODULE, join_node, [Site, JoinToSite, _JoinToNode, _]}) ->
  #{ Site := SState
   , JoinToSite := #{cluster := Cluster}
   } = Sites,
  S#{sites := Sites#{Site := SState#{cluster := Cluster}}}.

precondition(_, _) ->
    true.

postcondition(PrevState, Call, Result) ->
  CurrentState = next_state(PrevState, Result, Call),
  case Call of
    {call, ?MODULE, consume, _} ->
      %% FIXME
      Result =:= ok;
    _ ->
      true
  end and check_invariants(CurrentState).

%%--------------------------------------------------------------------
%% Misc.
%%--------------------------------------------------------------------

check_invariants(_CurrentState) ->
  true.

node_name(Site, Suffix) ->
  binary_to_atom(<<Site/binary, "_", Suffix/binary>>).

set_running(Site, IsRunning, S = #{sites := Sites0}) ->
  Sites = maps:update_with(
            Site,
            fun(SS) ->
                SS#{running := IsRunning}
            end,
            Sites0),
  S#{sites := Sites}.
