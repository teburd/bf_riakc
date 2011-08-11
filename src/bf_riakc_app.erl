%% @author Tom Burdick <thomas.burdick@gmail.com>
%% @copyright 2011 Tom Burdick
%% @doc Riak Client Pool Application.

-module(bf_riakc_app).
-behaviour(application).
-export([start/2, stop/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([setup_app/0, teardown_app/0]).
-endif.

start(_StartType, _StartArgs) ->
    io:format(user, "starting bf_riakc~n", []),
    application:load(bf_riakc),
    %{ok, Host} = application:get_env(bf_riakc, host),
    %{ok, Port} = application:get_env(bf_riakc, port),
    %{ok, Connections} = application:get_env(bf_riakc, connections),
    Host = "127.0.0.1",
    Port = 8081,
    Connections = 8,
    io:format(user, "starting connection pool size ~p to ~p:~p~n", [Connections, Host, Port]),
    ht_sup:start_link(bf_riakc, Connections, riakc_pb_socket,
        start_link, [Host, Port]).

stop(_State) ->
    ok.


-ifdef(TEST).

setup_app() ->
    ?debugHere,
    application:set_env(bf_riakc, host, "localhost"),
    application:set_env(bf_riakc, port, 8087),
    application:set_env(bf_riakc, connections, 8),
    ?debugHere,
    ?debugHere,
    start([], []),
    ?debugHere,
    Fun = fun(C) -> riakc_pb_socket:ping(C) end,
    ?assertEqual(pong, bf_riakc:execute(Fun)),
    ok.

teardown_app() ->
    application:stop(bf_riakc),
    application:stop(riakc_pb_client),
    application:unset_env(bf_riakc, host),
    application:unset_env(bf_riakc, port),
    application:unset_env(bf_riakc, connections),
    ?debugHere,
    ok.

-endif.
