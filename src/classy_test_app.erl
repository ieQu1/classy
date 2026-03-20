%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_test_app).

-behavior(classy_test_fixture).

%% behavior callbacks:
-export([init_per_node/4, cleanup_per_node/4]).

-export_type([conf/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type conf() :: #{ app := atom()
                 , env => map()
                 , start => boolean()
                 }.

%%================================================================================
%% behavior callbacks
%%================================================================================

init_per_node(Site, Node, Conf, State) ->
  Defaults = #{ env   => #{}
              , start => true
              },
  #{ app   := App
   , env   := Env
   , start := Start
   } = maps:merge(Defaults, Conf),
  ok = classy_test_site:call(
         Site,
         fun() ->
             ok = application:load(App),
             maps:foreach(
               fun(K, V) ->
                   application:set_env(App, K, V)
               end,
               Env)
         end),
  case Start of
    true ->
      {ok, Started} = classy_test_site:call(Site, application, ensure_all_started, [App]);
    false ->
      Started = []
  end,
  {ok, State#{{?MODULE, App} => Started}}.

cleanup_per_node(Site, _Node, #{app := App}, State) ->
  #{{?MODULE, App} := Started} = State,
  classy_test_site:call(
    Site,
    fun() ->
        lists:foreach(
          fun application:stop/1,
          lists:reverse(Started))
    end).
