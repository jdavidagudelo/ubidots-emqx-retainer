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

-module(ubidots_emqx_retainer).

-behaviour(gen_server).

-include("ubidots_emqx_retainer.hrl").

-include_lib("emqx/include/emqx.hrl").

-include_lib("emqx/include/logger.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

-logger_header("[Retainer]").

-export([start_link/1]).

-export([load/1, unload/0]).

-export([on_session_subscribed/5]).

-define(POOL_REACTOR, pool_reactor).

-define(POOL_CORE, pool_core).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {stats_fun, stats_timer, expiry_timer}).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

load(Env) ->
    ecpool:start_pool(?POOL_REACTOR,
                      ubidots_emqx_reactor_redis_cli,
                      ubidots_emqx_retainer_ecpool:get_ecpool_reactor_options(Env)
                          ++ Env),
    ecpool:start_pool(?POOL_CORE,
                      ubidots_emqx_core_redis_cli,
                      ubidots_emqx_retainer_ecpool:get_ecpool_ubidots_options(Env)
                          ++ Env),
    Config = #{pool_reactor => ?POOL_REACTOR,
               pool_core => ?POOL_CORE},
    emqx:hook('session.subscribed',
              fun on_session_subscribed/5,
              [Env, Config]).

unload() ->
    emqx:unhook('session.subscribed',
                {?MODULE, on_session_subscribed}).

on_session_subscribed(_, _, #{share := ShareName}, _Env,
                      _Config)
    when ShareName =/= undefined ->
    ok;
on_session_subscribed(_, Topic,
                      #{rh := Rh, is_new := IsNew}, Env, Config) ->
    case Rh =:= 0 orelse Rh =:= 1 andalso IsNew of
        true ->
            emqx_pool:async_submit(fun dispatch/4,
                                   [self(), Topic, Env, Config]);
        _ -> ok
    end.

%% @private
dispatch(Pid, Topic, Env,
         #{pool_reactor := PoolReactor,
           pool_core := PoolCore}) ->
    NewMessages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               Env,
                                                                               PoolReactor,
                                                                               PoolCore),
    dispatch_ubidots_message(NewMessages, Pid).

dispatch_ubidots_message([], _) -> ok;
dispatch_ubidots_message([Msg = #message{topic = Topic}
                          | Rest],
                         Pid) ->
    Pid ! {deliver, Topic, Msg},
    dispatch_ubidots_message(Rest, Pid).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Start the retainer
-spec start_link(Env ::
                     list()) -> emqx_types:startlink_ret().

start_link(Env) ->
    gen_server:start_link({local, ?MODULE},
                          ?MODULE,
                          [Env],
                          []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([_]) ->
    StatsFun = emqx_stats:statsfun('retained.count',
                                   'retained.max'),
    {ok, StatsTimer} = timer:send_interval(timer:seconds(1),
                                           stats),
    State = #state{stats_fun = StatsFun,
                   stats_timer = StatsTimer},
    {ok, State}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(stats, State) ->
    {noreply, State, hibernate};
handle_info(expire, State) ->
    {noreply, State, hibernate};
handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.
