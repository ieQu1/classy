%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Test fixture behavior.
%%
%% Test fixture behavior allows to define reusable test environment
%% setup routines, for example creating a working directory or
%% starting an OTP application.
%%
%% == Behavior callbacks ==
%%
%% <itemize>
%% <li>`init_per_cluster' / `cleanup_per_cluster': executed by
%% `classy_test_cluster' during cluster initialization and
%% termination.</li>
%% <li>`init_per_site' / `cleanup_per_site': executed by
%% `classy_test_site' when the site is created or destroyed. Note:
%% during execution of these callbacks the site is stopped. </li>
%% <li>`init_per_node' / `cleanup_per_node': executed by
%% `classy_test_site' when node of the site is started or stopped.
%% </li>
%% </itemize>

-module(classy_test_fixture).

%% API:
-export([ init_per_cluster/1
        , cleanup_per_cluster/3

        , init_per_site/3
        , cleanup_per_site/4

        , init_per_node/4
        , cleanup_per_node/4

        , exit_reason_to_success/1
        , defaults/1
        ]).

-export_type([t/0, conf/0, state/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type conf() :: term().

-type state() :: map().

-type t() :: {module(), conf()}.

-callback init_per_cluster(conf(), state()) -> {ok, state()} | {error, _}.

-callback cleanup_per_cluster(conf(), _Success :: boolean(), state()) -> ok | {error, _}.

-callback init_per_site(classy:site(), conf(), state()) -> {ok, state()} | {error, _}.

-callback cleanup_per_site(classy:site(), _Success :: boolean(), conf(), state()) -> ok | {error, _}.

-callback init_per_node(classy:site(), node(), conf(), state()) -> {ok, state()} | {error, _}.

-callback cleanup_per_node(classy:site(), node(), conf(), state()) -> ok | {error, _}.

-optional_callbacks([ init_per_cluster/2
                    , cleanup_per_cluster/3
                    , init_per_site/3
                    , cleanup_per_site/4
                    , init_per_node/4
                    , cleanup_per_node/4
                    ]).

%%================================================================================
%% API functions
%%================================================================================

-spec init_per_cluster([t()]) -> {ok, state()} | {error, _}.
init_per_cluster(Fixtures) ->
  Ret = fold_init(
          fun({Module, Conf}, Acc) ->
              safe_init_call(Module, init_per_cluster, [Conf, Acc])
          end,
          #{},
          Fixtures),
  case Ret of
    {ok, _} ->
      Ret;
    {error, OkFixtures, State, Reason} ->
      cleanup_per_cluster(OkFixtures, false, State),
      {error, Reason}
  end.

-spec cleanup_per_cluster([t()], boolean(), state()) -> ok.
cleanup_per_cluster(Fixtures, Success, State) ->
  lists:foreach(
    fun({Module, Conf}) ->
        safe_cleanup_call(Module, cleanup_per_cluster, [Conf, Success, State])
    end,
    lists:reverse(Fixtures)).

-spec init_per_site([t()], classy:site(), state()) -> {ok, state()} | {error, _}.
init_per_site(Fixtures, Site, State0) ->
  Ret = fold_init(
          fun({Module, Conf}, Acc) ->
              safe_init_call(Module, init_per_site, [Site, Conf, Acc])
          end,
          State0,
          Fixtures),
  case Ret of
    {ok, _} ->
      Ret;
    {error, OkFixtures, State, Reason} ->
      cleanup_per_site(OkFixtures, Site, false, State),
      {error, Reason}
  end.

-spec cleanup_per_site([t()], classy:site(), boolean(), state()) -> ok | {error, _}.
cleanup_per_site(Fixtures, Site, Success, State) ->
  lists:foreach(
    fun({Module, Conf}) ->
        safe_cleanup_call(Module, cleanup_per_site, [Site, Success, Conf, State])
    end,
    lists:reverse(Fixtures)).

-spec init_per_node([t()], classy:site(), node(), state()) -> {ok, state()} | {error, _}.
init_per_node(Fixtures, Site, Node, State0) ->
  Ret = fold_init(
          fun({Module, Conf}, Acc) ->
              safe_init_call(Module, init_per_node, [Site, Node, Conf, Acc])
          end,
          State0,
          Fixtures),
  case Ret of
    {ok, _} ->
      Ret;
    {error, OkFixtures, State, Reason} ->
      cleanup_per_node(OkFixtures, Site, Node, State),
      {error, Reason}
  end.

-spec cleanup_per_node([t()], classy:site(), node(), state()) -> ok | {error, _}.
cleanup_per_node(Fixtures, Site, Node, State) ->
  lists:foreach(
    fun({Module, Conf}) ->
        safe_cleanup_call(Module, cleanup_per_node, [Site, Node, Conf, State])
    end,
    lists:reverse(Fixtures)).

-spec exit_reason_to_success(_) -> boolean().
exit_reason_to_success(Reason) ->
  case Reason of
    normal   -> true;
    shutdown -> true;
    _        -> false
  end.

defaults(TestCase) ->
  [ {classy_test_workdir, #{testcase => TestCase}}
  , {classy_test_code_path, #{}}
  , {classy_test_cover, #{}}
  ].

%%================================================================================
%% Internal functions
%%================================================================================

safe_init_call(Module, Fun, Args) ->
  try
    %% This call will implicitly load the module:
    Exports = Module:module_info(exports),
    case lists:member({Fun, length(Args)}, Exports) of
      true ->
        Ret = apply(Module, Fun, Args),
        case Ret of
          {ok, M} when is_map(M) -> ok;
          {error, _}             -> ok;
          _                      -> error({invalid_return, Ret})
        end,
        Ret;
      false ->
        {ok, lists:last(Args)}
    end
  catch
    EC:Err:Stack ->
      {error, #{ EC         => Err
               , stacktrace => Stack
               , callback   => {Module, Fun}
               }}
  end.

safe_cleanup_call(Module, Fun, Args) ->
  try
    %% This call will implicitly load the module:
    Exports = Module:module_info(exports),
    case lists:member({Fun, length(Args)}, Exports) of
      true ->
        _ = apply(Module, Fun, Args),
        ok;
      false ->
        ok
    end
  catch
    EC:Err:Stack ->
      logger:error(#{ EC         => Err
                    , stacktrace => Stack
                    , callback   => {Module, Fun}
                    })
  end.

-spec fold_init(fun(({module(), conf()}, state()) -> {ok, state()} | {error, Reason}), state(), [t()]) ->
        {ok, state()} | {error, [t()], state(), Reason}.
fold_init(Fun, State, Fixtures) ->
  fold_init(Fun, State, Fixtures, []).

fold_init(_Fun, State, [], _OkFixtures) ->
  {ok, State};
fold_init(Fun, State0, [Fixture | Rest], OkFixtures) ->
  case Fun(Fixture, State0) of
    {ok, State} ->
      fold_init(Fun, State, Rest, [Fixture | OkFixtures]);
    {error, Reason} ->
      {error, lists:reverse(OkFixtures), State0, Reason}
  end.
