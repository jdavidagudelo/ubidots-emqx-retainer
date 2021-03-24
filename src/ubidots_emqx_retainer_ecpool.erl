-module(ubidots_emqx_retainer_ecpool).

-author("jdavidagudelo").

-export([start_pools/3]).

start_pools(PoolReactor, PoolCore, Env) ->
    ecpool:start_pool(PoolReactor, ubidots_emqx_reactor_redis_cli, get_ecpool_reactor_options(Env) ++ Env),
    ecpool:start_pool(PoolCore, ubidots_emqx_core_redis_cli, get_ecpool_ubidots_options(Env) ++ Env).

get_ecpool_reactor_options(Env) ->
    [{pool_size, proplists:get_value(reactor_cache_pool_size, Env, 10)},
     {pool_type, proplists:get_value(reactor_cache_pool_type, Env, round_robin)},
     {auto_reconnect, proplists:get_value(reactor_cache_reconnect, Env, 3)}].

get_ecpool_ubidots_options(Env) ->
    [{pool_size, proplists:get_value(ubidots_cache_pool_size, Env, 10)},
     {pool_type, proplists:get_value(ubidots_cache_pool_type, Env, round_robin)},
     {auto_reconnect, proplists:get_value(ubidots_cache_reconnect, Env, 3)}].
