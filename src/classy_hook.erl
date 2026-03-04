%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_hook).

%% API:
-export([ create_table/0
        , insert/3
        , foreach/2
        , all/2
        ]).

-export_type([ hookpoint/0
             , prio/0
             ]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(tab, ?MODULE).

-type hookpoint() :: atom().

-type prio() :: integer().

%%================================================================================
%% API functions
%%================================================================================

create_table() ->
  ets:new(?tab, [named_table, ordered_set, public, {keypos, 1}]).

-spec insert(hookpoint(), fun(), prio()) -> ok.
insert(Hookpoint, Hook, Prio) when is_atom(Hookpoint), is_integer(Prio), is_function(Hook) ->
  ets:insert(?tab, {{Hookpoint, -Prio, Hook}}),
  ok.

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
