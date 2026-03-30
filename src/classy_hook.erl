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
        , fold/3
        , all/2
        , first_match/2
        ]).

-export_type([ hookpoint/0
             , prio/0
             , hook/0
             ]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

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
        classy_node:maybe_init_the_site(undefined)
    end,
    -100),
  classy:post_kick(
    fun(OldCluster, Local, Intent) ->
        ?tp(info, classy_kicked_from_cluster,
            #{ old_cluster => OldCluster
             , local => Local
             , intent => Intent
             }),
        %% Re-initialize the local cluster upon getting kicked:
        Intent =/= join andalso
          classy_node:maybe_init_the_site(Local)
    end,
    -100),
  %% Info logging:
  classy:on_create_site(
    fun(Site) ->
        ?tp(info, classy_create_new_site,
            #{ local => Site
             })
    end,
    100),
  classy:on_create_cluster(
    fun(Cluster, Site) ->
        ?tp(info, classy_create_new_cluster,
            #{ cluster => Cluster
             , local => Site
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
             , local => Local
             })
    end,
    -100),
  classy:on_membership_change(
    fun(Cluster, Local, Remote, Member) ->
        Kind = case Member of
                 true -> classy_member_join;
                 false -> classy_member_leave
               end,
        ?tp(notice, Kind,
            #{ cluster => Cluster
             , local => Local
             , remote => Remote
             })
    end,
    100),
  classy:run_level(
    fun(From, To) ->
        ?tp(info, classy_change_run_level,
            #{ from => From
             , to => To
             })
    end,
    -100),
  %% User initialization:
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
%% Errors are ignored (logged).
-spec foreach(hookpoint(), list()) -> ok.
foreach(Hookpoint, Args) ->
  lists:foreach(
    fun(Hook) ->
        safe_apply(Hookpoint, Hook, Args)
    end,
    hooks(Hookpoint)).

%% @doc Fold over all functions registered in `Hookpoint'.
%% Accumulator argument is appended to the `Args' list.
%%
%% Errors are ignored (logged).
-spec fold(hookpoint(), list(), A) -> A.
fold(Hookpoint, Args, Acc0) ->
  lists:foldl(
    fun(Hook, Acc1) ->
        case safe_apply(Hookpoint, Hook, Args ++ [Acc1]) of
          {ok, Acc} ->
            Acc;
          error ->
            Acc1
        end
    end,
    Acc0,
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
          case safe_apply(Hookpoint, Hook, Args) of
            {ok, ok}           -> ok;
            {ok, {error, Err}} -> throw({found, Err});
            {ok, Res}          -> throw({found, {invalid_result, Res}});
            error              -> throw({found, callback_crashed})
          end
      end,
      hooks(Hookpoint)),
    ok
  catch
    {found, Err} -> {error, Err}
  end.

%% @doc Return result of the first hook that returned `{ok, _}' for
%% a given set of arguments.
-spec first_match(hookpoint(), list()) -> {ok, _Val} | undefined.
first_match(Hookpoint, Args) ->
  try
    lists:foreach(
      fun(Hook) ->
          case safe_apply(Hookpoint, Hook, Args) of
            {ok, {ok, Val}} -> throw({found, Val});
            _               -> ok
          end
      end,
      hooks(Hookpoint)),
    undefined
  catch
    {found, Err} -> {ok, Err}
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

-spec safe_apply(hookpoint(), fun(), list()) -> {ok, _Val} | error.
safe_apply(HookPoint, Fun, A) ->
  try
    {ok, apply(Fun, A)}
  catch
    EC:Err:Stack ->
      ?tp(warning, classy_hook_failure,
          #{ EC        => Err
           , stack     => Stack
           , hook      => Fun
           , hookpoint => HookPoint
           }),
      error
  end.
