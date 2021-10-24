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

get_variables_from_topic(Pool, Type, Topic) ->
    {ok, Result} = ubidots_emqx_reactor_redis_cli:get_variables_from_topic(Pool, Type, Topic),
    Result.

get_values_variables(Pool, Type, VariablesData) ->
    {ok, Result} = ubidots_emqx_core_redis_cli:get_values_variables(Pool, Type, VariablesData),
    Result.

get_values_from_topic(Topic, Env, PoolReactor, PoolCore) ->
    UbidotsRedisType = proplists:get_value(ubidots_cache_type, Env, single),
    ReactorRedisType = proplists:get_value(reactor_cache_type, Env, single),
    VariablesData = get_variables_from_topic(PoolReactor, ReactorRedisType, Topic),
    Values = get_values_variables(PoolCore, UbidotsRedisType, VariablesData),
    Values.

get_messages([]) -> [];
get_messages([Topic, Value | Rest]) ->
    NewMessage = emqx_message:make(Topic, Value),
    [NewMessage | get_messages(Rest)].

get_retained_messages_from_topic(Topic, Env, PoolReactor, PoolCore) ->
    Values = get_values_from_topic(Topic, Env, PoolReactor, PoolCore),
    get_messages(Values).
