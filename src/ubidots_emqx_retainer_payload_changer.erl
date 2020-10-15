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
-export([get_retained_messages_from_topic/1]).

get_file_path_local(LocalFilePath) ->
  {ok, FilePath} = file:get_cwd(),
  filename:join([FilePath, LocalFilePath]).

get_retainer_configuration() ->
  {ok, FilePath} = file:get_cwd(),
  FilePathConfiguration = filename:join([FilePath, "retainer_changer", "retainer_changer.conf"]),
  {ok, [Options | _]} = file:consult(FilePathConfiguration),
  Options.


get_reactor_redis_client(Options) ->
  Host = maps:get(reactor_cache_host_name, Options, "127.0.0.1"),
  Port = maps:get(reactor_cache_port, Options, 6379),
  Database = maps:get(reactor_cache_database, Options, 1),
  Password = maps:get(reactor_cache_password, Options, ""),
  {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
  RedisClient.

get_ubidots_redis_client(Options) ->
  Host = maps:get(ubidots_cache_host_name, Options, "127.0.0.1"),
  Port = maps:get(ubidots_cache_port, Options, 6379),
  Database = maps:get(ubidots_cache_database, Options, 1),
  Password = maps:get(ubidots_cache_password, Options, ""),
  {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
  RedisClient.

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

get_values_from_topic(Topic) ->
  Options = get_retainer_configuration(),
  ReactorScriptFilePath = maps:get(reactor_cache_get_subscription_variables_from_mqtt_topic_script_file_path, Options, ""),
  UbidotsScriptFilePath = maps:get(ubidots_cache_get_values_variables_script_file_path, Options, ""),
  ReactorRedisClient = get_reactor_redis_client(Options),
  UbidotsRedisClient = get_ubidots_redis_client(Options),
  ReactorScriptData = get_lua_script_from_file(get_file_path_local(ReactorScriptFilePath)),
  UbidotsScriptData = get_lua_script_from_file(get_file_path_local(UbidotsScriptFilePath)),
  VariablesData = get_variables_from_topic(ReactorRedisClient, ReactorScriptData, Topic),
  Values = get_values_variables(UbidotsRedisClient, UbidotsScriptData, VariablesData),
  Values.

get_messages([]) ->
  [];
get_messages([Topic, Value | Rest]) ->
  NewMessage = emqx_message:make(Topic, Value),
  [NewMessage | get_messages(Rest)].

get_retained_messages_from_topic(Topic) ->
  Values = get_values_from_topic(Topic),
  get_messages(Values).


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
  Options = get_retainer_configuration(),
  ReactorRedisClient = get_reactor_redis_client(Options),
  UbidotsRedisClient = get_ubidots_redis_client(Options),
  initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
  Result = get_values_from_topic("/v1.6/users/token/devices/d1/+/lv"),
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
  Options = get_retainer_configuration(),
  ReactorRedisClient = get_reactor_redis_client(Options),
  UbidotsRedisClient = get_ubidots_redis_client(Options),
  initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
  Result = get_values_from_topic("/v1.6/users/token/devices/d1/+"),
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
  Options = get_retainer_configuration(),
  ReactorRedisClient = get_reactor_redis_client(Options),
  UbidotsRedisClient = get_ubidots_redis_client(Options),
  initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
  [Topic, Value | _Rest] = get_values_from_topic("/v1.6/users/token/devices/d1/v1/lv"),
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
  Options = get_retainer_configuration(),
  ReactorRedisClient = get_reactor_redis_client(Options),
  UbidotsRedisClient = get_ubidots_redis_client(Options),
  initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
  [Topic, Value | _Rest] = get_values_from_topic("/v1.6/users/token/devices/d1/v1"),
  eredis:q(ReactorRedisClient, ["FLUSHDB"]),
  eredis:q(UbidotsRedisClient, ["FLUSHDB"]),
  [
    ?_assertEqual(<<"/v1.6/devices/d1/v1">>, Topic),
    ?_assertEqual(
      <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}">>, Value)
  ].

retainer_empty_test_() ->
  Result = get_values_from_topic("/v1.6/users/token/devices/d1/v1"),

  Options = get_retainer_configuration(),
  ReactorRedisClient = get_reactor_redis_client(Options),
  UbidotsRedisClient = get_ubidots_redis_client(Options),
  eredis:q(ReactorRedisClient, ["FLUSHDB"]),
  eredis:q(UbidotsRedisClient, ["FLUSHDB"]),
  [
    ?_assertEqual(Result, [])
  ].