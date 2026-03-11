%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(classy_ct).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% @doc Get all the test cases in a CT suite.
all(Suite) ->
  lists:usort([F || {F, 1} <- Suite:module_info(exports),
                    string:substr(atom_to_list(F), 1, 2) == "t_"
              ]).

cleanup(Testcase) ->
  ct:pal("Cleaning up after ~p...", [Testcase]).

-type env() :: [{atom(), atom(), term()}].

-type start_spec() ::
        #{ name       := atom()
         , node       := node()
         , env        := env()
         , number     := integer()
         , code_paths := [file:filename_all()]
         }.

-type node_spec() :: #{ name => atom()
                      , env => env()
                      , code_paths => [file:filename_all()]
                      }.

-type run_info() :: #{ node := node()
                     , pid  := pid()
                     , _ => _
                     }.

%% @doc Generate cluster config with all necessary connectivity
%% options, that should be able to run on the localhost
-spec cluster([node_spec()], env()) -> [start_spec()].
cluster(Specs0, CommonEnv) ->
  Specs1 = lists:zip(Specs0, lists:seq(1, length(Specs0))),
  expand_node_specs(Specs1, CommonEnv).

-spec start_cluster(node | classy, [node_spec()]) -> [run_info()].
start_cluster(node, Specs) ->
  Nodes = [start_peer(node, I) || I <- Specs],
  Nodes;
start_cluster(classy, Specs) ->
  Ret = start_cluster(node, Specs),
  [start_classy(I) || I <- Ret],
  Ret.

start_peer(
  node,
  #{ name := Name
   , env := Env
   , code_paths := CodePaths
   , cover := Cover
   , workdir := WorkDir
   } = Spec
 ) ->
  filelib:ensure_path(WorkDir),
  CommonBeamOpts = "+S 1:1 " % We want VMs to only occupy a single core
    "-kernel inet_dist_listen_min 3000 " % Avoid collisions with gen_rpc ports
    "-kernel inet_dist_listen_max 3050 ",
  {ok, Pid, Node} = ?CT_PEER(#{ name => Name
                              , longnames => true
                              , peer_down => stop
                              , host => "127.0.0.1"
                                %% , args => string:words(CommonBeamOpts)
                              , shutdown => halt
                              , wait_boot => 5_000
                              }),
  erlang:register(Node, Pid),
  Ret = Spec#{node => Node, pid => Pid},
  Self = filename:dirname(code:which(?MODULE)),
  [erpc:call(Node, code, add_patha, [Path]) || Path <- [Self|CodePaths]],
  FormatterConfig = #{ template => [[header, node], "\n", msg, "\n"]
                     , legacy_header => false
                     , single_line => false
                     },
  rpc(Ret, logger, update_formatter_config, [default, FormatterConfig]),
  LogLevel = list_to_atom(os:getenv("LOG_LEVEL", "notice")),
  rpc(Ret, logger, update_primary_config, [#{level => LogLevel}]),
  rpc(Ret, logger, update_handler_config, [default, #{level => LogLevel}]),
  [{ok, _} = cover:start([Node]) || Cover],
  setenv(Ret, Env),
  ok = snabbkaffe:forward_trace(Node),
  Ret;
start_peer(classy, Spec) ->
  Ret = start_peer(node, Spec),
  ok = rpc(Ret, mria, start, []),
  ok = rpc(Ret, mria_transaction_gen, init, []),
  Ret.

teardown_cluster(Specs) ->
  ?tp(notice, teardown_cluster, #{}),
  [ok = stop_peer(I) || I <- Specs],
  [remove_workdir(I) || I <- Specs],
  ok.

start_classy(Spec) ->
  ok = rpc(Spec, application, set_env, [classy, setup_hooks, {?MODULE, setup_hooks, []}]),
  {ok, _} = rpc(Spec, application, ensure_all_started, [classy]),
  Spec.

setup_hooks() ->
  ok.

master_code_paths() ->
  lists:filter(fun is_lib/1, code:get_path()).

wait_running(Node) ->
  wait_running(Node, 30000).

wait_running(Node, Timeout) when Timeout < 0 ->
  throw({wait_timeout, Node});

wait_running(Node, Timeout) ->
  case rpc(Node, mria, is_running, [Node, mria]) of
    true  -> ok;
    false -> timer:sleep(100),
             wait_running(Node, Timeout - 100)
  end.

stop_peer(Spec = #{name := Name}) ->
  Node = list_to_atom(atom_to_list(Name) ++ "@127.0.0.1"),
  case whereis(Node) of
    Pid when is_pid(Pid) ->
      rpc(Spec#{node => Node}, application, stop, [classy]),
      ok = cover:stop([Node]),
      peer:stop(Pid);
    undefined ->
      ok
  end.

remove_workdir(#{workdir := Dir}) ->
  file:del_dir_r(Dir).

host() ->
  [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path(CodePaths) ->
  string:join(["-pa" | CodePaths], " ").

is_lib(Path) ->
  string:prefix(Path, code:lib_dir()) =:= nomatch.

run_on(Pid, Fun) ->
  run_on(Pid, Fun, []).

run_on(Pid, Fun, Args) ->
  %% Sending closures over erlang distribution is wrong, but for
  %% test purposes it should be ok.
  case rpc(Pid, erlang, apply, [Fun, Args]) of
    {badrpc, Err} ->
      error(Err);
    Result ->
      Result
  end.

set_network_delay(N) ->
  ok = file:write_file("/tmp/nemesis", integer_to_list(N) ++ "us\n").

vals_to_csv(L) ->
  string:join([lists:flatten(io_lib:format("~p", [N])) || N <- L], ",") ++ "\n".

setenv(Node, Env) ->
  [rpc(Node, application, set_env, [App, Key, Val]) || {App, Key, Val} <- Env].

expand_node_specs(Specs, CommonEnv) ->
  lists:map(
    fun({CustomSpec, Num}) ->
        WorkDir = filename:join("workdir", integer_to_list(Num)),
        %% If code path is not default, we have to disable
        %% cover. It will replace custom paths with
        %% cover-compiled paths, and generally mess things up:
        Cover = not maps:is_key(code_paths, CustomSpec),
        DefaultSpec = #{ name => gen_node_name(Num)
                       , env => [{classy, table_dir, WorkDir}]
                       , code_paths => master_code_paths()
                       , num => Num
                       , cover => Cover
                       , workdir => WorkDir
                       },
        maps:update_with(env,
                         fun(Env) -> CommonEnv ++ Env end,
                         maps:merge(DefaultSpec, CustomSpec))
    end,
    Specs).

gen_node_name(N) ->
    list_to_atom("classy" ++ integer_to_list(N)).

get_txid() ->
    case mnesia:get_activity_id() of
        {_, TID, _} ->
            TID
    end.

start_dist() ->
    ensure_epmd(),
    case net_kernel:start('ct@127.0.0.1', #{hidden => true}) of
        {ok, _Pid} -> ok;
        {error, {already_started, _}} -> ok
    end.

ensure_epmd() ->
    open_port({spawn, "epmd"}, []).

shim(Mod, Fun, Args) ->
    group_leader(self(), whereis(init)),
    apply(Mod, Fun, Args).

rpc(#{node := Node}, Mod, Fun, Args) ->
    erpc:call(Node, ?MODULE, shim, [Mod, Fun, Args]).

mailbox() ->
    receive M -> [M | mailbox()] after 0 -> [] end.
