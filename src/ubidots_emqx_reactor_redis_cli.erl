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

-module(ubidots_emqx_reactor_redis_cli).

-behaviour(ecpool_worker).

-include_lib("emqx/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([connect/1, get_variables_from_topic/4]).

%%--------------------------------------------------------------------
%% Redis Connect/Query
%%--------------------------------------------------------------------

connect(Env) ->
    Host = proplists:get_value(reactor_cache_host_name, Env, "127.0.0.1"),
    Port = proplists:get_value(reactor_cache_port, Env, 6379),
    Database = proplists:get_value(reactor_cache_database, Env, 2),
    Password = proplists:get_value(reactor_cache_password, Env, ""),
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

get_topic_type(match, match) ->
    both;
get_topic_type(match, nomatch) ->
    value;
get_topic_type(nomatch, match) ->
    last_value;
get_topic_type(nomatch, nomatch) ->
    none.

get_mqtt_topic_kind(Topic) ->
    LastValuePattern = "^/(v1.6|v2.0)/users/[^/]+/devices/((([a-zA-Z0-9_:.-]+|[+])/((([a-zA-Z0-9_:.-]+|[+])/(lv|#|[+]))|#)|#)|#)$",
    ValuePattern = "^/(v1.6|v2.0)/users/[^/]+/devices/((([a-zA-Z0-9_:.-]+|[+])/((([a-zA-Z0-9_:.-]+|[+]))|#)|#)|#)$",
    IsLastValueTopic = re:run(Topic, LastValuePattern, [{capture, all_names}]),
    IsValueTopic =  re:run(Topic, ValuePattern, [{capture, all_names}]),
    get_topic_type(IsValueTopic, IsLastValueTopic).

get_variable_label([]) ->
    "*";
get_variable_label(["+" | _Rest]) ->
    "*";
get_variable_label(["#" | _Rest]) ->
    "*";
get_variable_label([VariableLabel | _Rest]) ->
    VariableLabel.

get_device_label("+") ->
    "*";
get_device_label("#") ->
    "*";
get_device_label(DeviceLabel) ->
    DeviceLabel.

decode_mqtt_topic(Topic) ->
    [_, Version, _, Token, _, DeviceLabelPart | Rest] = re:split(Topic, "/"),
    VariableLabel = get_variable_label(Rest),
    DeviceLabel = get_device_label(DeviceLabelPart),
    [Version, Token, DeviceLabel, VariableLabel].

can_view_value_device(_, _, "all", _, _, _) ->
    true;
can_view_value_device(Pool, Type, PermissionsType, OwnerId, DeviceLabel, Token) ->
    DeviceHashSetKey = string:concat("reactor_variables/", OwnerId),
    DeviceLabelKey = string:concat("/", DeviceLabel),
    TokenKey = string:concat("reactor_devices_with_permissions/view_value/", Token),
    DeviceId = ecpool:with_client(Pool, fun (RedisClient) -> eredis:q(RedisClient, ["HGET", DeviceHashSetKey, DeviceLabelKey]) end),
    CanViewValueDevice = ecpool:with_client(Pool, fun (RedisClient) -> eredis:q(RedisClient, ["SISMEMBER", TokenKey, DeviceId]) end),
    CanViewValueDevice.

get_variables_topic(Pool, Type, Topic) ->
    TopicKind = get_mqtt_topic_kind(Topic),
    [Version, Token, DeviceLabel, VariableLabel] = decode_mqtt_topic(Topic),
    TokenHashSetKey = string:concat("reactor_tokens/", binary_to_list(token)),

    %OwnerId = ecpool:with_client(Pool, fun (RedisClient) -> eredis:q(RedisClient, ["HGET", TokenHashSetKey, "/owner_id"]) end),
    %PermissionsType = ecpool:with_client(Pool, fun (RedisClient) -> eredis:q(RedisClient, ["HGET", TokenHashSetKey, "/permissions_type"]) end),
    
    ok.

get_topics_variable(Pool, Type, Topic) ->
    TopicKind = get_mqtt_topic_kind(Topic),
    
    TokenHashSetKey = string:concat("reactor_tokens/", token),
    ok.

get_variables_from_topic(Pool, Type, ScriptData, Topic) ->
    get_variables_topic(Pool, Type, Topic),
    Args = ["EVAL", ScriptData, 1, Topic],
    case Type of
        cluster -> eredis_cluster:q(Pool, Args);
        _ -> ecpool:with_client(Pool, fun (RedisClient) -> eredis:q(RedisClient, Args) end)
    end.

decode_mqtt_topic_binary_test() ->
    [
        ?_assert(decode_mqtt_topic(<<"/v2.6/users/token/devices/d1/v1/lv">>) =:= ["v1.6", "token", "d1", "v1"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/v1") =:= ["v1.6", "token", "d1", "v1"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/+/lv") =:= ["v1.6", "token", "d1", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/+") =:= ["v1.6", "token", "d1", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/#") =:= ["v1.6", "token", "d1", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/+/v1") =:= ["v1.6", "token", "*", "v1"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/+/v1/lv") =:= ["v1.6", "token", "*", "v1"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/#") =:= ["v1.6", "token", "*", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/+/+") =:= ["v1.6", "token", "*", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/+/#") =:= ["v1.6", "token", "*", "*"])  
    ].

decode_mqtt_topic_test() ->
    [
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/v1/lv") =:= ["v1.6", "token", "d1", "v1"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/v1") =:= ["v1.6", "token", "d1", "v1"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/+/lv") =:= ["v1.6", "token", "d1", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/+") =:= ["v1.6", "token", "d1", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/d1/#") =:= ["v1.6", "token", "d1", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/+/v1") =:= ["v1.6", "token", "*", "v1"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/+/v1/lv") =:= ["v1.6", "token", "*", "v1"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/#") =:= ["v1.6", "token", "*", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/+/+") =:= ["v1.6", "token", "*", "*"]),
        ?_assert(decode_mqtt_topic("/v1.6/users/token/devices/+/#") =:= ["v1.6", "token", "*", "*"])  
    ].

get_mqtt_topic_invalid_test() -> 
    ?_assert(1 == 2),
    ?_assert(get_mqtt_topic_kind("invalid_topic") =:= value),
    ?_assert(get_mqtt_topic_kind("/v1.x/users/token/devices/d1/v1/lv") =:= value),
    ?_assert(get_mqtt_topic_kind("/v1.6/user/token/devices/d1/v1/lv") =:= none).

get_mqtt_topic_v1_kind_test() ->
    [?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/d1/v1/lv") =:= last_value),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/d1/v1") =:= value),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/d1/+/lv") =:= last_value),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/d1/+") =:= value),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/d1/#") =:= both),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/+/v1") =:= value),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/+/v1/lv") =:= last_value),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/#") =:= both),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/+/+") =:= both),
    ?_assert(get_mqtt_topic_kind("/v1.6/users/token/devices/+/#") =:= both)].


get_mqtt_topic_v2_kind_test() ->
    [?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/d1/v1/lv") =:= last_value),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/d1/v1") =:= value),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/d1/+/lv") =:= last_value),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/d1/+") =:= value),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/d1/#") =:= both),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/+/v1") =:= value),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/+/v1/lv") =:= last_value),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/#") =:= both),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/+/+") =:= both),
    ?_assert(get_mqtt_topic_kind("/v2.0/users/token/devices/+/#") =:= both)].