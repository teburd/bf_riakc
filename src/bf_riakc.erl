%% @author Tom Burdick <thomas.burdick@gmail.com>
%% @copyright 2011 Tom Burdick
%% @doc Riak Client Pool API.

-module(bf_riakc).
-export([execute/1, delete/2, get/2, list_keys/1, put/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Finds the next available connection pid from the pool and calls
%% `Fun(Pid)'. Returns `{ok, Value}' if the call was successful, and
%% `{error, any()}' otherwise. If no connection could be found, a new connection
%% will be established.
%% ```
%% > bf_riakc:execute(fun(C) -> riakc_pb_socket:ping(C) end).
%% {ok,pong}
%% '''
-spec execute(Fun::fun((Connection::pid()) -> Result::any())) -> Result::any(). 
execute(Fun) ->
    hottub:execute(bf_riakc, Fun).
 
%% @doc Delete `Key' from `Bucket'.
-spec delete(binary(), binary()) -> ok.
delete(Bucket, Key) ->
    execute(fun(C) -> riakc_pb_socket:delete(C, Bucket, Key) end).

%% @doc Returns the value associated with `Key' in `Bucket' as `{ok, binary()}'.
%% If an error was encountered or the value was not present, returns
%% `{error, any()}'.
-spec get(binary(), binary()) -> {ok, binary()} | {error, any()}.
get(Bucket, Key) ->
    Fun =
        fun(C) ->
            case riakc_pb_socket:get(C, Bucket, Key) of
                {ok, O} -> riakc_obj:get_value(O);
                {error, E} -> {error, E}
            end
        end,
    execute(Fun).

%% @doc Returns the list of keys in `Bucket' as `{ok, list()}'. If an error was
%% encountered, returns `{error, any()}'.
-spec list_keys(binary()) -> {ok, list()} | {error, any()}.
list_keys(Bucket) ->
    Fun = fun(C) -> riakc_pb_socket:list_keys(C, Bucket) end,
    execute(Fun).

%% @doc Associates `Key' with `Value' in `Bucket'. If `Key' already exists in
%% `Bucket', an update will be preformed.
-spec put(binary(), binary(), binary()) -> ok.
put(Bucket, Key, Value) ->
    Fun =
        fun(C) ->
            case riakc_pb_socket:get(C, Bucket, Key) of
                {ok, O} ->
                    O2 = riakc_obj:update_value(O, Value),
                    riakc_pb_socket:put(C, O2);
                {error, _} ->
                    O = riakc_obj:new(Bucket, Key, Value),
                    riakc_pb_socket:put(C, O)
            end
        end,
    execute(Fun),
    ok.

-ifdef(TEST).

%% @doc Ensure an application has started.
ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

bf_riakc_test_() ->
    {setup,
        fun() -> ensure_started(bf_riakc), ok end,
        fun(_) -> ok end,
        [fun basics/0]}.

basics() ->
    PingFun = fun(C) -> riakc_pb_socket:ping(C) end,
    N0 = <<"_never_exists">>,
    {B, K, V1, V2} = {<<"groceries">>, <<"mine">>, <<"eggs">>, <<"toast">>},
    ?assertEqual(pong, execute(PingFun)),
    ?assertEqual(ok, delete(B, K)),
    ?assertMatch({ok, []}, list_keys(N0)),
    ?assertMatch({error, _}, get(N0, N0)),
    ?assertEqual(ok, put(B, K, V1)),
    ?assertEqual(V1, get(B, K)),
    ?assertEqual(ok, put(B, K, V2)),
    ?assertEqual(V2, get(B, K)),
    ?assertEqual({ok, [K]}, list_keys(B)),
    ?assertEqual(ok, delete(B, K)),
    ?assertEqual({ok, []}, list_keys(B)).

-endif.
