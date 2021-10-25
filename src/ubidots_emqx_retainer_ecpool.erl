-module(ubidots_emqx_retainer_ecpool).

-author("jdavidagudelo").

-export([start_pools/3]).

init_redis_cluster(Pool, Env, ServerKey, PasswordKey, PoolSizeKey, ReconnectKey) ->
    eredis_cluster:start(),
    Fun = fun (S) ->
                  case string:split(S, ":", trailing) of
                      [Domain] -> {Domain, 6379};
                      [Domain, Port] -> {Domain, list_to_integer(Port)}
                  end
          end,
    Server = proplists:get_value(ServerKey, Env, 10),
    Servers = string:tokens(Server, ","),
    Password = proplists:get_value(PasswordKey, Env, ""),
    eredis_cluster:start_pool(Pool,
                              [{pool_size, proplists:get_value(PoolSizeKey, Env, 10)},
                               {password, Password},
                               {servers, [Fun(S1) || S1 <- Servers]},
                               {auto_reconnect, proplists:get_value(ReconnectKey, Env, 3)}]).

start_pools(PoolReactor, PoolCore, Env) ->
    start_reactor_pool(PoolReactor, Env),
    start_core_pool(PoolCore, Env).

start_reactor_pool(PoolReactor, Env) ->
    Type = proplists:get_value(reactor_cache_type, Env, single),
    case Type of
        cluster ->
            init_redis_cluster(PoolReactor,
                               Env,
                               reactor_cache_server,
                               reactor_cache_password,
                               reactor_cache_pool_size,
                               reactor_cache_reconnect);
        single -> ecpool:start_pool(PoolReactor, ubidots_emqx_reactor_redis_cli, get_ecpool_reactor_options(Env) ++ Env)
    end.

start_core_pool(PoolCore, Env) ->
    Type = proplists:get_value(ubidots_cache_type, Env, cluster),
    case Type of
        cluster ->
            init_redis_cluster(PoolCore,
                               Env,
                               ubidots_cache_server,
                               ubidots_cache_password,
                               ubidots_cache_pool_size,
                               ubidots_cache_reconnect);
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
