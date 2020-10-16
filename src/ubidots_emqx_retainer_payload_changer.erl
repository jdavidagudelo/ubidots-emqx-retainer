%%%-------------------------------------------------------------------
%%% @author jdavidagudelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Sep 2019 1:20 p. m.
%%%-------------------------------------------------------------------
-module(ubidots_emqx_retainer_payload_changer).
-author("jdavidagudelo").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([get_retained_messages_from_topic/2]).

-export([get_reactor_redis_client/1, get_ubidots_redis_client/1, initialize_mqtt_cache/5, get_test_env/0]).


get_reactor_redis_client(Env) ->
  Host = proplists:get_value(reactor_cache_host_name, Env, "127.0.0.1"),
  Port = proplists:get_value(reactor_cache_port, Env, 6379),
  Database = proplists:get_value(reactor_cache_database, Env, 2),
  Password = proplists:get_value(reactor_cache_password, Env, ""),
  {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
  RedisClient.

get_ubidots_redis_client(Env) ->
  Host = proplists:get_value(ubidots_cache_host_name, Env, "127.0.0.1"),
  Port = proplists:get_value(ubidots_cache_port, Env, 6379),
  Database = proplists:get_value(ubidots_cache_database, Env, 1),
  Password = proplists:get_value(ubidots_cache_password, Env, ""),
  {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
  RedisClient.

get_lua_script_from_base64("") ->
  "";
get_lua_script_from_base64(Data) ->
  base64:decode(Data).

get_lua_script_data_from_env_result("", FilePath) ->
  get_lua_script_from_file(FilePath);
get_lua_script_data_from_env_result(Data, _) ->
  Data.

get_lua_script_from_file(FilePath) ->
  {ok, FileData} = file:read_file(FilePath),
  FileData.

get_variables_from_topic(RedisClient, ScriptData, Topic) ->
  {ok, Result} = eredis:q(RedisClient, ["EVAL", ScriptData, 1, Topic]),
  Result.

get_values_variables(RedisClient, ScriptData, VariablesData) ->
  VariablesDataArray = array:from_list(VariablesData),
  Args = ["EVAL", ScriptData, array:size(VariablesDataArray)] ++ VariablesData,
  {ok, Result} = eredis:q(RedisClient, Args),
  Result.

get_values_from_topic(Topic, Env) ->
  ReactorScriptFilePath = proplists:get_value(reactor_cache_get_subscription_variables_from_mqtt_topic_script_file_path, Env, ""),
  UbidotsScriptFilePath = proplists:get_value(ubidots_cache_get_values_variables_script_file_path, Env, ""),
  ReactorRedisClient = get_reactor_redis_client(Env),
  UbidotsRedisClient = get_ubidots_redis_client(Env),
  ReactorScriptData = get_lua_script_data_from_env_result(get_lua_script_from_base64(proplists:get_value(reactor_cache_get_subscription_variables_from_mqtt_topic_script_base64, Env, "")), ReactorScriptFilePath),
  UbidotsScriptData = get_lua_script_data_from_env_result(get_lua_script_from_base64(proplists:get_value(ubidots_cache_get_values_variables_script_base64, Env, "")), UbidotsScriptFilePath),
  VariablesData = get_variables_from_topic(ReactorRedisClient, ReactorScriptData, Topic),
  Values = get_values_variables(UbidotsRedisClient, UbidotsScriptData, VariablesData),
  Values.

get_messages([]) ->
  [];
get_messages([Topic, Value | Rest]) ->
  NewMessage = emqx_message:make(Topic, Value),
  [NewMessage | get_messages(Rest)].

get_retained_messages_from_topic(Topic, Env) ->
  Values = get_values_from_topic(Topic, Env),
  get_messages(Values).


get_test_env() ->
  [
    {reactor_cache_host_name, "redis"},
    {reactor_cache_port, 6379},
    {reactor_cache_database, 2},
    {reactor_cache_password, ""},
    {ubidots_cache_host_name, "redis"},
    {ubidots_cache_port, 6379},
    {ubidots_cache_database, 1},
    {ubidots_cache_password, ""},
    {reactor_cache_get_subscription_variables_from_mqtt_topic_script_file_path, "retainer_test_data/get_subscription_variables_from_mqtt_topic.lua"},
    {ubidots_cache_get_values_variables_script_file_path, "retainer_test_data/get_values_variables.lua"}
    ].

initialize_variables(_, _, _, _, []) ->
  ok;
initialize_variables(ReactorRedisClient, UbidotsRedisClient, OwnerId, DeviceLabel, [VariableLabel, VariableId | Rest]) ->
  eredis:q(ReactorRedisClient, ["HSET", "reactor_variable_data/" ++ VariableId, "/variable_label", VariableLabel]),
  eredis:q(ReactorRedisClient, ["HSET", "reactor_variables/" ++ OwnerId, "/" ++ DeviceLabel ++ "/" ++ VariableLabel, VariableId]),
  ValueData = "{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}",
  eredis:q(UbidotsRedisClient, ["SET", "last_value_variables_json:" ++ VariableId, ValueData]),
  eredis:q(UbidotsRedisClient, ["SET", "last_value_variables_string:" ++ VariableId, "11.1"]),
  initialize_variables(ReactorRedisClient, UbidotsRedisClient, OwnerId, DeviceLabel, Rest),
  ok.

initialize_devices(_, _, _, _, []) ->
  ok;
initialize_devices(ReactorRedisClient, UbidotsRedisClient, OwnerId, Token, [DeviceLabel, DeviceId, Variables | Rest]) ->
  eredis:q(ReactorRedisClient, ["HSET", "reactor_device_data/" ++ DeviceId, "/device_label", DeviceLabel]),
  eredis:q(ReactorRedisClient, ["HSET", "reactor_variables/" ++ OwnerId, "/" ++ DeviceLabel, DeviceId]),
  eredis:q(ReactorRedisClient, ["SADD", "reactor_devices_with_permissions/view_value/" ++ Token, DeviceId]),
  initialize_variables(ReactorRedisClient, UbidotsRedisClient, OwnerId, DeviceLabel, Variables),
  initialize_devices(ReactorRedisClient, UbidotsRedisClient, OwnerId, Token, Rest),
  ok.

initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, Token, OwnerId, Devices) ->
  eredis:q(ReactorRedisClient, ["HSET", "reactor_tokens/" ++ Token, "/owner_id", OwnerId]),
  eredis:q(ReactorRedisClient, ["HSET", "reactor_tokens/" ++ Token, "/permissions_type", "device"]),
  initialize_devices(ReactorRedisClient, UbidotsRedisClient, OwnerId, Token, Devices),
  ok.

retainer_multiple_lv_test_() ->
  Devices = ["d1", "d1_id", ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
    "d2", "d2_id", ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
  Env = get_test_env(),
  ReactorRedisClient = get_reactor_redis_client(Env),
  UbidotsRedisClient = get_ubidots_redis_client(Env),
  initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
  Result = get_values_from_topic("/v1.6/users/token/devices/d1/+/lv", Env),
  eredis:q(ReactorRedisClient, ["FLUSHDB"]),
  eredis:q(UbidotsRedisClient, ["FLUSHDB"]),
  [
    ?_assertEqual([<<"/v1.6/devices/d1/v1/lv">>, <<"11.1">>,
      <<"/v1.6/devices/d1/v2/lv">>, <<"11.1">>,
      <<"/v1.6/devices/d1/v3/lv">>, <<"11.1">>], Result)
  ].

retainer_multiple_test_() ->
  Devices = ["d1", "d1_id", ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
    "d2", "d2_id", ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
  Env = get_test_env(),
  ReactorRedisClient = get_reactor_redis_client(Env),
  UbidotsRedisClient = get_ubidots_redis_client(Env),
  initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
  Result = get_values_from_topic("/v1.6/users/token/devices/d1/+", Env),
  eredis:q(ReactorRedisClient, ["FLUSHDB"]),
  eredis:q(UbidotsRedisClient, ["FLUSHDB"]),
  [
    ?_assertEqual([<<"/v1.6/devices/d1/v1">>,
      <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}">>,
      <<"/v1.6/devices/d1/v2">>,
      <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}">>,
      <<"/v1.6/devices/d1/v3">>,
      <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}">>], Result)
  ].


retainer_lv_test_() ->
  Devices = ["d1", "d1_id", ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
    "d2", "d2_id", ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
  Env = get_test_env(),
  ReactorRedisClient = get_reactor_redis_client(Env),
  UbidotsRedisClient = get_ubidots_redis_client(Env),
  initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
  [Topic, Value | _Rest] = get_values_from_topic("/v1.6/users/token/devices/d1/v1/lv", Env),
  eredis:q(ReactorRedisClient, ["FLUSHDB"]),
  eredis:q(UbidotsRedisClient, ["FLUSHDB"]),
  [
    ?_assertEqual(<<"/v1.6/devices/d1/v1/lv">>, Topic),
    ?_assertEqual(
      <<"11.1">>, Value)
  ].

retainer_test_() ->
  Devices = ["d1", "d1_id", ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
    "d2", "d2_id", ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
  Env = get_test_env(),
  ReactorRedisClient = get_reactor_redis_client(Env),
  UbidotsRedisClient = get_ubidots_redis_client(Env),
  initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
  [Topic, Value | _Rest] = get_values_from_topic("/v1.6/users/token/devices/d1/v1", Env),
  eredis:q(ReactorRedisClient, ["FLUSHDB"]),
  eredis:q(UbidotsRedisClient, ["FLUSHDB"]),
  [
    ?_assertEqual(<<"/v1.6/devices/d1/v1">>, Topic),
    ?_assertEqual(
      <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}">>, Value)
  ].

retainer_empty_test_() ->
  Env = get_test_env(),
  Result = get_values_from_topic("/v1.6/users/token/devices/d1/v1", Env),
  ReactorRedisClient = get_reactor_redis_client(Env),
  UbidotsRedisClient = get_ubidots_redis_client(Env),
  eredis:q(ReactorRedisClient, ["FLUSHDB"]),
  eredis:q(UbidotsRedisClient, ["FLUSHDB"]),
  [
    ?_assertEqual(Result, [])
  ].