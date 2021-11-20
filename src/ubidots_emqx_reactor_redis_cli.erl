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

-include_lib("eunit/include/eunit.hrl").

-export([connect/1, get_variables_from_topic/3]).

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
            {error, Reason};
        {error, Reason = {authentication_error, _}} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

get_topic_type(match, match) -> both;
get_topic_type(match, nomatch) -> value;
get_topic_type(nomatch, match) -> last_value;
get_topic_type(nomatch, nomatch) -> none.

get_mqtt_topic_kind(Topic) ->
    LastValuePattern = "^/(v1.6|v2.0)/users/[^/]+/devices/((([a-zA-Z0-9_:.-]+|[+])/((([a-zA-Z0-9_:.-]+|[+])/(lv"
                       "|#|[+]))|#)|#)|#)$",
    ValuePattern = "^/(v1.6|v2.0)/users/[^/]+/devices/((([a-zA-Z0-9_:.-]+|[+])/((([a-zA-Z0-9_:.-]+|[+]))|#)"
                   "|#)|#)$",
    IsLastValueTopic = re:run(Topic, LastValuePattern, [{capture, all_names}]),
    IsValueTopic = re:run(Topic, ValuePattern, [{capture, all_names}]),
    get_topic_type(IsValueTopic, IsLastValueTopic).

get_variable_label([]) -> "*";
get_variable_label(["+" | _Rest]) -> "*";
get_variable_label(["#" | _Rest]) -> "*";
get_variable_label([VariableLabel | _Rest]) -> VariableLabel.

get_device_label("+") -> "*";
get_device_label("#") -> "*";
get_device_label(DeviceLabel) -> DeviceLabel.

decode_mqtt_topic(Topic) ->
    [_, Version, _, Token, _, DeviceLabelPart | Rest] = re:split(Topic, "/", [{return, list}]),
    VariableLabel = get_variable_label(Rest),
    DeviceLabel = get_device_label(DeviceLabelPart),
    [Version, Token, DeviceLabel, VariableLabel].

execute_redis_command(Pool, Type, Args) ->
    case Type of
        cluster ->
            {ok, Result} = eredis_cluster:q(Pool, Args),
            Result;
        single ->
            {ok, Result} = ecpool:with_client(Pool, fun (RedisClient) -> eredis:q(RedisClient, Args) end),
            Result
    end.

can_view_value_device(_Pool, _Type, "all", _OwnerId, _DeviceLabel, _Token) -> true;
can_view_value_device(Pool, Type, _, OwnerId, DeviceLabel, Token) ->
    DeviceHashSetKey = "reactor_variables/" ++ OwnerId,
    DeviceLabelKey = "/" ++ DeviceLabel,
    TokenKey = "reactor_devices_with_permissions/view_value/" ++ Token,
    DeviceId = execute_redis_command(Pool, Type, ["HGET", DeviceHashSetKey, DeviceLabelKey]),
    CanViewValueDevice = execute_redis_command(Pool, Type, ["SISMEMBER", TokenKey, DeviceId]),
    binary_to_list(CanViewValueDevice).

get_topics_mqtt_by_topic(last_value, Topic, VariableId) -> [atom_to_list(last_value), Topic ++ "/lv", VariableId];
get_topics_mqtt_by_topic(value, Topic, VariableId) -> [atom_to_list(value), Topic, VariableId];
get_topics_mqtt_by_topic(both, Topic, VariableId) ->
    get_topics_mqtt_by_topic(last_value, Topic, VariableId) ++ get_topics_mqtt_by_topic(value, Topic, VariableId).

get_topics_mqtt(DeviceLabel, VariableLabel, VariableId, TopicKind, Version) ->
    Topic = "/" ++ Version ++ "/devices/" ++ DeviceLabel ++ "/" ++ VariableLabel,
    get_topics_mqtt_by_topic(TopicKind, Topic, VariableId).

get_mqtt_topics_variable_label(_DeviceLabel, _VariableId, [], _TopicKind, _Version) -> [];
get_mqtt_topics_variable_label(DeviceLabel, VariableId, [VariableLabel], TopicKind, Version) ->
    get_topics_mqtt(DeviceLabel, VariableLabel, VariableId, TopicKind, Version).

get_mqtt_topics_variable_key(VariableKey, VariableId, DeviceLabels, TopicKind, Version) ->
    [_, DeviceLabel | Rest] = re:split(VariableKey, "/", [{return, list}]),
    case lists:member(DeviceLabel, DeviceLabels) of
        true -> get_mqtt_topics_variable_label(DeviceLabel, VariableId, Rest, TopicKind, Version);
        false -> []
    end.

get_mqtt_topics_by_hash_set([], _DeviceLabels, _TopicKind, _Version) -> [];
get_mqtt_topics_by_hash_set([VariableKey, VariableId | Rest], DeviceLabels, TopicKind, Version) ->
    get_mqtt_topics_variable_key(VariableKey, VariableId, DeviceLabels, TopicKind, Version) ++
        get_mqtt_topics_by_hash_set(Rest, DeviceLabels, TopicKind, Version).

get_mqtt_topics_by_device_labels(Pool, Type, DeviceLabels, OwnerId, TopicKind, Version) ->
    ReactorVariablesKey = "reactor_variables/" ++ OwnerId,
    VariablesData = execute_redis_command(Pool, Type, ["HGETALL", ReactorVariablesKey]),
    get_mqtt_topics_by_hash_set(VariablesData, DeviceLabels, TopicKind, Version).

get_device_labels_by_device_ids(_Pool, _Type, []) -> [];
get_device_labels_by_device_ids(Pool, Type, [DeviceId | Rest]) ->
    DeviceHashSetKey = "reactor_device_data/" ++ binary_to_list(DeviceId),
    DeviceLabel = execute_redis_command(Pool, Type, ["HGET", DeviceHashSetKey, "/device_label"]),
    [binary_to_list(DeviceLabel)] ++ get_device_labels_by_device_ids(Pool, Type, Rest).

get_device_labels(Pool, Type, Token) ->
    DeviceIdsKey = "reactor_devices_with_permissions/view_value/" ++ Token,
    DeviceIds = execute_redis_command(Pool, Type, ["SMEMBERS", DeviceIdsKey]),
    get_device_labels_by_device_ids(Pool, Type, DeviceIds).

get_mqtt_topics_by_label(Pool, Type, "1", "*", "*", OwnerId, Token, TopicKind, Version) ->
    DeviceLabels = get_device_labels(Pool, Type, Token),
    get_mqtt_topics_by_device_labels(Pool, Type, DeviceLabels, OwnerId, TopicKind, Version);
get_mqtt_topics_by_label(Pool, Type, "1", DeviceLabel, "*", OwnerId, _, TopicKind, Version) ->
    get_mqtt_topics_by_device_labels(Pool, Type, [DeviceLabel], OwnerId, TopicKind, Version);
get_mqtt_topics_by_label(Pool, Type, "1", DeviceLabel, VariableLabel, OwnerId, _, TopicKind, Version) ->
    VariableIdHashSetKey = "reactor_variables/" ++ OwnerId,
    VariableIdKey = "/" ++ DeviceLabel ++ "/" ++ VariableLabel,
    VariableId = execute_redis_command(Pool, Type, ["HGET", VariableIdHashSetKey, VariableIdKey]),
    get_topics_mqtt(DeviceLabel, VariableLabel, VariableId, TopicKind, Version);
get_mqtt_topics_by_label(_Pool, _Type, _CanViewDevice, _DeviceLabel, _VariableLabel, _OwnerId, _Token, _TopicKind,
                         _Version) ->
    [].

get_variables_topic(Pool, Type, Topic) ->
    TopicKind = get_mqtt_topic_kind(Topic),
    [Version, Token, DeviceLabel, VariableLabel] = decode_mqtt_topic(Topic),
    TokenHashSetKey = "reactor_tokens/" ++ Token,
    OwnerId = execute_redis_command(Pool, Type, ["HGET", TokenHashSetKey, "/owner_id"]),
    PermissionsType = execute_redis_command(Pool, Type, ["HGET", TokenHashSetKey, "/permissions_type"]),
    CanViewDevice = can_view_value_device(Pool,
                                          Type,
                                          binary_to_list(PermissionsType),
                                          binary_to_list(OwnerId),
                                          DeviceLabel,
                                          Token),
    get_mqtt_topics_by_label(Pool,
                             Type,
                             CanViewDevice,
                             DeviceLabel,
                             VariableLabel,
                             binary_to_list(OwnerId),
                             Token,
                             TopicKind,
                             Version).

get_variables_from_topic(Pool, Type, Topic) ->
    Result = {ok, get_variables_topic(Pool, Type, Topic)},
    Result.

get_mqtt_topics_test() ->
    ["last_value", "/v1.6/devices/d1/v1/lv", "variable_id"] = get_topics_mqtt_by_topic(last_value,
                                                                                       "/v1.6/devices/d1/v1",
                                                                                       "variable_id"),
    ["value", "/v1.6/devices/d1/v1", "variable_id"] = get_topics_mqtt_by_topic(value,
                                                                               "/v1.6/devices/d1/v1",
                                                                               "variable_id"),
    ["last_value", "/v1.6/devices/d1/v1/lv", "variable_id", "value", "/v1.6/devices/d1/v1", "variable_id"] =
        get_topics_mqtt_by_topic(both, "/v1.6/devices/d1/v1", "variable_id").

decode_mqtt_topic_test() ->
    ["v1.6", "token", "d1", "v1"] = decode_mqtt_topic("/v1.6/users/token/devices/d1/v1/lv"),
    ["v1.6", "token", "d1", "v1"] = decode_mqtt_topic("/v1.6/users/token/devices/d1/v1"),
    ["v1.6", "token", "d1", "*"] = decode_mqtt_topic("/v1.6/users/token/devices/d1/+/lv"),
    ["v1.6", "token", "d1", "*"] = decode_mqtt_topic("/v1.6/users/token/devices/d1/+"),
    ["v1.6", "token", "d1", "*"] = decode_mqtt_topic("/v1.6/users/token/devices/d1/#"),
    ["v1.6", "token", "*", "v1"] = decode_mqtt_topic("/v1.6/users/token/devices/+/v1"),
    ["v1.6", "token", "*", "v1"] = decode_mqtt_topic("/v1.6/users/token/devices/+/v1/lv"),
    ["v1.6", "token", "*", "*"] = decode_mqtt_topic("/v1.6/users/token/devices/#"),
    ["v1.6", "token", "*", "*"] = decode_mqtt_topic("/v1.6/users/token/devices/+/+"),
    ["v1.6", "token", "*", "*"] = decode_mqtt_topic("/v1.6/users/token/devices/+/#").

get_mqtt_topic_invalid_test() ->
    none = get_mqtt_topic_kind("invalid_topic"),
    none = get_mqtt_topic_kind("/v1.x/users/token/devices/d1/v1/lv"),
    none = get_mqtt_topic_kind("/v1.6/user/token/devices/d1/v1/lv").

get_mqtt_topic_v1_kind_test() ->
    last_value = get_mqtt_topic_kind("/v1.6/users/token/devices/d1/v1/lv"),
    value = get_mqtt_topic_kind("/v1.6/users/token/devices/d1/v1"),
    last_value = get_mqtt_topic_kind("/v1.6/users/token/devices/d1/+/lv"),
    value = get_mqtt_topic_kind("/v1.6/users/token/devices/d1/+"),
    both = get_mqtt_topic_kind("/v1.6/users/token/devices/d1/#"),
    value = get_mqtt_topic_kind("/v1.6/users/token/devices/+/v1"),
    last_value = get_mqtt_topic_kind("/v1.6/users/token/devices/+/v1/lv"),
    both = get_mqtt_topic_kind("/v1.6/users/token/devices/#"),
    value = get_mqtt_topic_kind("/v1.6/users/token/devices/+/+"),
    both = get_mqtt_topic_kind("/v1.6/users/token/devices/+/#").

get_mqtt_topic_v2_kind_test() ->
    last_value = get_mqtt_topic_kind("/v2.0/users/token/devices/d1/v1/lv"),
    value = get_mqtt_topic_kind("/v2.0/users/token/devices/d1/v1"),
    last_value = get_mqtt_topic_kind("/v2.0/users/token/devices/d1/+/lv"),
    value = get_mqtt_topic_kind("/v2.0/users/token/devices/d1/+"),
    both = get_mqtt_topic_kind("/v2.0/users/token/devices/d1/#"),
    value = get_mqtt_topic_kind("/v2.0/users/token/devices/+/v1"),
    last_value = get_mqtt_topic_kind("/v2.0/users/token/devices/+/v1/lv"),
    both = get_mqtt_topic_kind("/v2.0/users/token/devices/#"),
    value = get_mqtt_topic_kind("/v2.0/users/token/devices/+/+"),
    both = get_mqtt_topic_kind("/v2.0/users/token/devices/+/#").
