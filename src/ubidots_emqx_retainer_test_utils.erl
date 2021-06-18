%%%-------------------------------------------------------------------
%%% @author jdavidagudelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Sep 2019 1:20 p. m.
%%%-------------------------------------------------------------------
-module(ubidots_emqx_retainer_test_utils).

-author("jdavidagudelo").

-include_lib("eunit/include/eunit.hrl").

-export([get_reactor_redis_client/1,
         get_ubidots_redis_client/1,
         initialize_mqtt_cache/6,
         get_test_env/0,
         get_redis_cluster_test_env/0,
         init_redis_cluster_ubidots/2,
         flushdb/3]).

get_reactor_redis_client(Env) ->
    Host = proplists:get_value(reactor_cache_host_name, Env, "127.0.0.1"),
    Port = proplists:get_value(reactor_cache_port, Env, 6379),
    Database = proplists:get_value(reactor_cache_database, Env, 2),
    Password = proplists:get_value(reactor_cache_password, Env, ""),
    {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
    RedisClient.

get_ubidots_redis_client(Env) ->
    Type = proplists:get_value(ubidots_cache_type, Env, single),
    Host = proplists:get_value(ubidots_cache_host_name, Env, "127.0.0.1"),
    Port = proplists:get_value(ubidots_cache_port, Env, 6379),
    Database = proplists:get_value(ubidots_cache_database, Env, 1),
    Password = proplists:get_value(ubidots_cache_password, Env, ""),
    case Type of
        single ->
            {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
            RedisClient;
        cluster -> ok
    end.

init_redis_cluster_ubidots(PoolCore, Env) ->
    eredis_cluster:start(),
    Fun = fun (S) ->
                  case string:split(S, ":", trailing) of
                      [Domain] -> {Domain, 6379};
                      [Domain, Port] -> {Domain, list_to_integer(Port)}
                  end
          end,
    Server = proplists:get_value(ubidots_cache_server, Env, 10),
    Servers = string:tokens(Server, ","),
    eredis_cluster:start_pool(PoolCore,
                              [{pool_size, proplists:get_value(ubidots_cache_pool_size, Env, 10)},
                               {servers, [Fun(S1) || S1 <- Servers]},
                               {auto_reconnect, proplists:get_value(ubidots_cache_reconnect, Env, 3)}]).

get_redis_cluster_test_env() ->
    [{reactor_cache_host_name, "redis"},
     {reactor_cache_port, 6379},
     {reactor_cache_database, 2},
     {reactor_cache_password, ""},
     {ubidots_cache_host_name, "redis"},
     {ubidots_cache_port, 6379},
     {ubidots_cache_database, 1},
     {ubidots_cache_type, cluster},
     {ubidots_cache_server,
      "rediscluster:7000,rediscluster:7001,rediscluster:7002,rediscluster:7003,rediscluster:70"
      "04,rediscluster:7005"},
     {ubidots_cache_password, "bitnami"},
     {reactor_cache_get_subscription_variables_from_mqtt_topic_script_file_path,
      "retainer_test_data/get_subscription_variables_from_mqtt_topic.lua"},
     {ubidots_cache_get_values_variables_script_file_path, "retainer_test_data/get_values_variables.lua"}].

get_test_env() ->
    [{reactor_cache_host_name, "redis"},
     {reactor_cache_port, 6379},
     {reactor_cache_database, 2},
     {reactor_cache_password, ""},
     {ubidots_cache_host_name, "redis"},
     {ubidots_cache_port, 6379},
     {ubidots_cache_database, 1},
     {ubidots_cache_password, ""},
     {reactor_cache_get_subscription_variables_from_mqtt_topic_script_file_path,
      "retainer_test_data/get_subscription_variables_from_mqtt_topic.lua"},
     {ubidots_cache_get_values_variables_script_file_path, "retainer_test_data/get_values_variables.lua"}].

flushdb(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType) ->
    case UbidotsRedisType of
        single ->
            eredis:q(UbidotsRedisClient, ["FLUSHDB"]),
            eredis:q(ReactorRedisClient, ["FLUSHDB"]);
        cluster -> eredis_cluster:q(UbidotsRedisClient, ["FLUSHDB"])
    end.

initialize_variables(_, _, _, _, _, []) -> ok;
initialize_variables(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType, OwnerId, DeviceLabel,
                     [VariableLabel, VariableId | Rest]) ->
    eredis:q(ReactorRedisClient, ["HSET", "reactor_variable_data/" ++ VariableId, "/variable_label", VariableLabel]),
    eredis:q(ReactorRedisClient,
             ["HSET", "reactor_variables/" ++ OwnerId, "/" ++ DeviceLabel ++ "/" ++ VariableLabel, VariableId]),
    ValueData = "{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": "
                "11}",
    case UbidotsRedisType of
        single ->
            eredis:q(UbidotsRedisClient, ["SET", "last_value_variables_json:" ++ VariableId, ValueData]),
            eredis:q(UbidotsRedisClient, ["SET", "last_value_variables_string:" ++ VariableId, "11.1"]),
            initialize_variables(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType, OwnerId, DeviceLabel, Rest);
        cluster ->
            eredis_cluster:q(UbidotsRedisClient, ["SET", "last_value_variables_json:" ++ VariableId, ValueData]),
            eredis_cluster:q(UbidotsRedisClient, ["SET", "last_value_variables_string:" ++ VariableId, "11.1"]),
            initialize_variables(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType, OwnerId, DeviceLabel, Rest)
    end.

initialize_devices(_, _, _, _, _, []) -> ok;
initialize_devices(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType, OwnerId, Token,
                   [DeviceLabel, DeviceId, Variables | Rest]) ->
    eredis:q(ReactorRedisClient, ["HSET", "reactor_device_data/" ++ DeviceId, "/device_label", DeviceLabel]),
    eredis:q(ReactorRedisClient, ["HSET", "reactor_variables/" ++ OwnerId, "/" ++ DeviceLabel, DeviceId]),
    eredis:q(ReactorRedisClient, ["SADD", "reactor_devices_with_permissions/view_value/" ++ Token, DeviceId]),
    initialize_variables(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType, OwnerId, DeviceLabel, Variables),
    initialize_devices(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType, OwnerId, Token, Rest),
    ok.

initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType, Token, OwnerId, Devices) ->
    eredis:q(ReactorRedisClient, ["HSET", "reactor_tokens/" ++ Token, "/owner_id", OwnerId]),
    eredis:q(ReactorRedisClient, ["HSET", "reactor_tokens/" ++ Token, "/permissions_type", "device"]),
    initialize_devices(ReactorRedisClient, UbidotsRedisClient, UbidotsRedisType, OwnerId, Token, Devices),
    ok.
