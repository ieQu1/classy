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

-module(classy_discovery_strategy).

-export([discover/2, lock/2, unlock/2, register/2, unregister/2]).

-export_type([options/0]).

-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Callback and type declarations
%%================================================================================

-type options() :: term().

-callback discover(options()) -> {ok, [node()]} | {error, term()}.

-callback lock(options()) -> ok | ignore | {error, term()}.

-callback unlock(options()) -> ok | ignore | {error, term()}.

-callback register(options()) -> ok | ignore | {error, term()}.

-callback unregister(options()) -> ok | ignore | {error, term()}.

%%================================================================================
%% API functions
%%================================================================================

-spec discover(module(), options()) -> {ok, [node()]} | {error, term()}.
discover(Mod, Options) ->
  safe_call(Mod, ?FUNCTION_NAME, Options).

-spec lock(module(), options()) -> ok | ignore | {error, term()}.
lock(Mod, Options) ->
  safe_call(Mod, ?FUNCTION_NAME, Options).

-spec unlock(module(), options()) -> ok | ignore | {error, term()}.
unlock(Mod, Options) ->
  safe_call(Mod, ?FUNCTION_NAME, Options).

-spec register(module(), options()) -> ok | ignore | {error, term()}.
register(Mod, Options) ->
  safe_call(Mod, ?FUNCTION_NAME, Options).

-spec unregister(module(), options()) -> ok | ignore | {error, term()}.
unregister(Mod, Options) ->
  safe_call(Mod, ?FUNCTION_NAME, Options).

%%================================================================================
%% Internal functions
%%================================================================================

safe_call(Module, Function, Options) ->
  try apply(Module, Function, Options)
  catch
    EC:Err:Stack ->
      ?tp(warning, classy_discovery_failure,
          #{ EC       => Err
           , stack    => Stack
           , module   => Module
           , function => Function
           , options  => Options
           }),
      {error, callback_crashed}
  end.
