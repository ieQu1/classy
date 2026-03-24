%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Module responsible for managing the hooks.
-module(classy_hook).

%% API:
-export([ init/0
        , insert/3
        , unhook/1
        , foreach/2
        , all/2
        ]).

-export_type([ hookpoint/0
             , prio/0
             , hook/0
             ]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(tab, ?MODULE).

-type hookpoint() :: atom().

-type prio() :: integer().

-opaque hook() :: tuple().

%%================================================================================
%% API functions
%%================================================================================

%% @private
init() ->
  ets:new(?tab, [named_table, ordered_set, public, {keypos, 1}]),
  %% Default initialization:
  classy:on_node_init(
    fun() ->
        ?tp(classy_on_node_init,
            #{ node => node()
             }),
        classy_node:maybe_init_the_site(undefined, undefined)
    end,
    -100),
  classy:post_kick(
    fun(_OldCluster, Local, Intent) ->
        %% Re-initialize the local cluster upon getting kicked:
        Intent =/= join andalso
          classy_node:maybe_init_the_site(undefined, Local)
    end,
    -100),
  %% Info logging:
  classy:on_create_site(
    fun(Site) ->
        ?tp(info, classy_create_new_site,
            #{ site => Site
             })
    end,
    100),
  classy:on_create_cluster(
    fun(Cluster) ->
        ?tp(info, classy_create_new_cluster,
            #{ cluster => Cluster
             }),
        ok
    end,
    100),
  classy:pre_join(
    fun(Cluster, Remote, Node, UserArg) ->
        ?tp(debug, classy_pre_join_node,
            #{ cluster => Cluster
             , remote => Remote
             , remote_node => Node
             , user_arg => UserArg
             }),
        ok
    end,
    100),
  classy:post_join(
    fun(Cluster, Local) ->
        ?tp(notice, classy_joined_cluster,
            #{ cluster => Cluster
             , local_site => Local
             })
    end,
    -100),
  classy:on_membership_change(
    fun(Cluster, _Local, Remote, Member) ->
        Kind = case Member of
                 true -> classy_member_join;
                 false -> classy_member_leave
               end,
        ?tp(notice, Kind,
            #{ cluster => Cluster
             , site => Remote
             })
    end,
    100),
  classy:run_level(
    fun(From, To) ->
        ?tp(notice, classy_change_run_level,
            #{ from => From
             , to => To
             })
    end,
    -100),
  %% User initializaiton:
  case application:get_env(classy, setup_hooks) of
    {ok, {Mod, Func, Args}} ->
      apply(Mod, Func, Args),
      ok;
    undefined ->
      ok
  end.

%% @private
-spec insert(hookpoint(), fun(), prio()) -> hook().
insert(Hookpoint, Hook, Prio) when is_atom(Hookpoint), is_integer(Prio), is_function(Hook) ->
  Key = {Hookpoint, -Prio, Hook},
  ets:insert(?tab, {Key}),
  Key.

%% @doc Remove a previosuly inserted hook
-spec unhook(hook()) -> ok.
unhook(Key) ->
  ets:delete(?tab, Key),
  ok.

%% @doc Execute all functions hooked into `Hookpoint'.
%%
%% Errors are ignored (logged)
-spec foreach(hookpoint(), list()) -> ok.
foreach(Hookpoint, Args) ->
  lists:foreach(
    fun(Hook) ->
        try apply(Hook, Args)
        catch
          EC:Err:Stack ->
            logger:warning(#{ EC => Err
                            , stack => Stack
                            , hook => Hookpoint
                            , reason => classy_hook_failure
                            })
        end
    end,
    hooks(Hookpoint)).

%% @doc Ensure that all functions hooked into `Hookpoint' return `ok'.
%%
%% If any of the function returns `{error, _}' or throws an exception,
%% this function returns `{error, _}'.
-spec all(hookpoint(), list()) -> ok | {error, _}.
all(Hookpoint, Args) ->
  try
    lists:foreach(
      fun(Hook) ->
          try apply(Hook, Args) of
            ok -> ok;
            {error, Err} -> throw({found, Err})
          catch
            EC:Err:Stack ->
              logger:warning(#{ EC => Err
                              , stack => Stack
                              , hook => Hookpoint
                              , reason => classy_hook_failure
                              }),
              throw({found, "Callback crashed"})
          end
      end,
      hooks(Hookpoint)),
    ok
  catch
    {found, Err} -> {error, Err}
  end.

%%================================================================================
%% Internal functions
%%================================================================================

hooks(Hookpoint) ->
  MS = { {{Hookpoint, '_', '$1'}}
       , []
       , ['$1']
       },
  ets:select(?tab, [MS]).
