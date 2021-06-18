%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ubidots_emqx_core_redis_cli).

-behaviour(ecpool_worker).

-include_lib("emqx/include/logger.hrl").

-export([connect/1, get_values_variables/4, get_values_variables/3]).

%%--------------------------------------------------------------------
%% Redis Connect/Query
%%--------------------------------------------------------------------

connect(Env) ->
    Host = proplists:get_value(ubidots_cache_host_name, Env, "127.0.0.1"),
    Port = proplists:get_value(ubidots_cache_port, Env, 6379),
    Database = proplists:get_value(ubidots_cache_database, Env, 1),
    Password = proplists:get_value(ubidots_cache_password, Env, ""),
    case eredis:start_link(Host, Port, Database, Password, 3000, 5000) of
        {ok, Pid} -> {ok, Pid};
        {error, Reason = {connection_error, _}} ->
            ?LOG(error, "[Redis] Can't connect to Redis server: Connection refused."),
            {error, Reason};
        {error, Reason = {authentication_error, _}} ->
            ?LOG(error, "[Redis] Can't connect to Redis server: Authentication failed."),
            {error, Reason};
        {error, Reason} ->
            ?LOG(error, "[Redis] Can't connect to Redis server: ~p", [Reason]),
            {error, Reason}
    end.

get_values_variables(Pool, Type, ScriptData, VariablesData) ->
    VariablesDataArray = array:from_list(VariablesData),
    Args = ["EVAL", ScriptData, array:size(VariablesDataArray)] ++ VariablesData,
    case Type of
        cluster -> eredis_cluster:q(Pool, Args);
        _ -> ecpool:with_client(Pool, fun (RedisClient) -> eredis:q(RedisClient, Args) end)
    end.

get_variable_key(ValueKind) ->
    case ValueKind of
        <<"value">> -> "last_value_variables_json:";
        <<"last_value">> -> "last_value_variables_string:"
    end.

get_value_by_key(Pool, VariableKey, Type) ->
    case Type of
        single -> ecpool:with_client(Pool, fun (RedisClient) -> eredis:q(RedisClient, ["GET", VariableKey]) end);
        cluster -> eredis_cluster:q(Pool, ["GET", VariableKey])
    end.

get_values_loop_redis(_Pool, _Type, []) ->
    [];
get_values_loop_redis(Pool, Type, [ValueKind, Topic, VariableId | RestData]) ->
    VariableKey = string:concat(get_variable_key(ValueKind), binary_to_list(VariableId)),
    {ok, Value} = get_value_by_key(Pool, VariableKey, Type),
    [Topic, Value] ++ get_values_loop_redis(Pool, Type, RestData).

get_values_variables(Pool, Type, VariablesData) ->
    {ok, get_values_loop_redis(Pool, Type, VariablesData)}.