%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Test fixture that configures and starts an OTP application.
%%
%% Configuration:
%%
%% <itemize>
%% <li>`app': OTP application</li>
%% <li>`env': Application environment variables to set,
%% represented as a key-value map.
%% Default is `#{}'.</li>
%% <li>`start': `true', start the application. `false', just load.
%% Default is `true'</li>
%% </itemize>
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

%% @private
init_per_node(Site, _Node, Conf, State) ->
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
             case application:load(App) of
               ok                           -> ok;
               {error, {already_loaded, _}} -> ok
             end,
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

%% @private
cleanup_per_node(Site, _Node, #{app := App}, State) ->
  #{{?MODULE, App} := Started} = State,
  classy_test_site:call(
    Site,
    fun() ->
        lists:foreach(
          fun application:stop/1,
          lists:reverse(Started))
    end).
