%%%-------------------------------------------------------------------
%%% @author jdavidagudelo
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Oct 2020 9:50 a. m.
%%%-------------------------------------------------------------------
-module(ubidots_emqx_retainer_SUITE).
-author("jdavidagudelo").

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([ubidots_emqx_retainer]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([ubidots_emqx_retainer]).

init_per_testcase(_TestCase, Config) ->
    application:stop(ubidots_emqx_retainer),
    application:ensure_all_started(ubidots_emqx_retainer),
    Config.


end_per_testcase(_TestCase, Config) ->
    application:stop(ubidots_emqx_retainer),
    Config.

t_retain_handling(_) ->
    Devices = ["d1", "d1_id", ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
    "d2", "d2_id", ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
    Env = ubidots_emqx_retainer_payload_changer:get_test_env(),
    ReactorRedisClient = ubidots_emqx_retainer_payload_changer:get_reactor_redis_client(Env),
    UbidotsRedisClient = ubidots_emqx_retainer_payload_changer:get_ubidots_redis_client(Env),
    ubidots_emqx_retainer_payload_changer:initialize_mqtt_cache(ReactorRedisClient, UbidotsRedisClient, "token", "owner_id", Devices),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"/v1.6/users/token/devices/d1/v1">>, [{qos, 0}, {rh, 0}]),
    ExpectedMessages = [#{ topic => <<"/v1.6/devices/d1/v1">>, payload => <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}">>}],
    Messages = receive_messages(1),
    ?assertEqual(1, length(Messages)),
    validate_messages(Messages, ExpectedMessages),
    ok = emqtt:disconnect(C1).


validate_messages([], []) ->
    ok;
validate_messages([ #{topic := Topic, payload := Payload} | Rest], [#{topic := ExpectedTopic, payload := ExpectedPayload} | ExpectedRest]) ->
    ?assertEqual(Topic, ExpectedTopic),
    ?assertEqual(Payload, ExpectedPayload),
    validate_messages(Rest, ExpectedRest).

receive_messages(Count) ->
    receive_messages(Count, []).
receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            ct:log("Msg: ~p ~n", [Msg]),
            io:fwrite("Publish? ~n", []),
            receive_messages(Count - 1, [Msg|Msgs]);
        Other ->
            ct:log("Other Msg: ~p~n",[Other]),
            io:fwrite("Other++? ~n", []),
            receive_messages(Count, Msgs)
    after 2000 ->
            Msgs
    end.