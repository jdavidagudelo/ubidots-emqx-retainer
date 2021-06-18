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
-export([get_retained_messages_from_topic/4]).

get_lua_script_from_base64("") -> "";
get_lua_script_from_base64(Data) -> base64:decode(Data).

get_lua_script_data_from_env_result("", FilePath) -> get_lua_script_from_file(FilePath);
get_lua_script_data_from_env_result(Data, _) -> Data.

get_lua_script_from_file(FilePath) ->
    {ok, FileData} = file:read_file(FilePath),
    FileData.

get_variables_from_topic(Pool, ScriptData, Topic) ->
    {ok, Result} = ubidots_emqx_reactor_redis_cli:get_variables_from_topic(Pool, single, ScriptData, Topic),
    Result.

get_values_variables(Pool, Type, VariablesData) ->
    {ok, Result} = ubidots_emqx_core_redis_cli:get_values_variables(Pool, Type, VariablesData),
    Result.

get_values_from_topic(Topic, Env, PoolReactor, PoolCore) ->
    UbidotsRedisType = proplists:get_value(ubidots_cache_type, Env, single),
    GetVariablesFromTopicScriptFilePath = reactor_cache_get_subscription_variables_from_mqtt_topic_script_file_path,
    ReactorScriptFilePath = proplists:get_value(GetVariablesFromTopicScriptFilePath, Env, ""),
    GetVariablesFromTopicScriptBase64 = reactor_cache_get_subscription_variables_from_mqtt_topic_script_base64,
    ReactorScriptBase64 = proplists:get_value(GetVariablesFromTopicScriptBase64, Env, ""),
    ReactorLuaScriptFromBase64 = get_lua_script_from_base64(ReactorScriptBase64),
    ReactorScriptData = get_lua_script_data_from_env_result(ReactorLuaScriptFromBase64, ReactorScriptFilePath),
    VariablesData = get_variables_from_topic(PoolReactor, ReactorScriptData, Topic),
    Values = get_values_variables(PoolCore, UbidotsRedisType, VariablesData),
    Values.

get_messages([]) -> [];
get_messages([Topic, Value | Rest]) ->
    NewMessage = emqx_message:make(Topic, Value),
    [NewMessage | get_messages(Rest)].

get_retained_messages_from_topic(Topic, Env, PoolReactor, PoolCore) ->
    Values = get_values_from_topic(Topic, Env, PoolReactor, PoolCore),
    get_messages(Values).
