-module(ubidots_emqx_retainer_ecpool).

-author("jdavidagudelo").

-export([start_pools/3, stop_pools/3]).

stop_pools(PoolReactor, PoolCore, Env) ->
    ok. 

init_redis_cluster() ->
    eredis_cluster:start(),
    {ok, _ } = eredis_cluster:start_pool(pool_core_test, 
        [{pool_size, 10},
    {password, "bitnami"},
     {servers, [{"192.168.100.10", 6380}, 
                {"192.168.100.10", 6381}, 
                {"192.168.100.10", 6382}, 
                {"192.168.100.10", 6383}, 
                {"192.168.100.10", 6384}, 
                {"192.168.100.10", 6385}]},
     {auto_reconnect, 3}]).

start_pools(PoolReactor, PoolCore, Env) ->
    ecpool:start_pool(PoolReactor, ubidots_emqx_reactor_redis_cli, get_ecpool_reactor_options(Env) ++ Env),
    start_core_pool(PoolCore, Env).


start_core_pool(PoolCore, Env) -> 
    Type =  proplists:get_value(ubidots_cache_type, Env, single),
    case Type of
        cluster -> ok;
        single -> ecpool:start_pool(PoolCore, ubidots_emqx_core_redis_cli, get_ecpool_ubidots_options(Env) ++ Env)
    end.

get_ecpool_reactor_options(Env) ->
    [{pool_size, proplists:get_value(reactor_cache_pool_size, Env, 10)},
     {pool_type, proplists:get_value(reactor_cache_pool_type, Env, round_robin)},
     {auto_reconnect, proplists:get_value(reactor_cache_reconnect, Env, 3)}].

get_ecpool_ubidots_options(Env) ->
    [{pool_size, proplists:get_value(ubidots_cache_pool_size, Env, 10)},
     {pool_type, proplists:get_value(ubidots_cache_pool_type, Env, round_robin)},
     {auto_reconnect, proplists:get_value(ubidots_cache_reconnect, Env, 3)}].
