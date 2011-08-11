%% @author Tom Burdick <thomas.burdick@gmail.com>
%% @copyright 2011 Tom Burdick
%% @doc Riak Client Pool API.

-module(bf_riakc).
-export([start_link/4, stop/1, execute/2, delete/3, get/3, list_keys/2, put/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Start a linked connection pool (supervisor).
-spec start_link(atom(), pos_integer(), string(), pos_integer()) -> {ok, pid()}.
start_link(Name, Connections, Host, Port) ->
    hottub:start_link(Name, Connections,
        riakc_pb_socket, start_link, [Host, Port]).

%% @doc Stop a connection pool.
-spec stop(atom()) -> ok.
stop(Name) ->
    hottub:stop(Name).

%% @doc Finds the next available connection pid from the pool and calls
%% `Fun(Pid)'. Returns `{ok, Value}' if the call was successful, and
%% `{error, any()}' otherwise. If no connection could be found, a new connection
%% will be established.
%% ```
%% > bf_riakc:execute(fun(C) -> riakc_pb_socket:ping(C) end).
%% {ok,pong}
%% '''
-spec execute(atom(), fun((pid()) -> any())) -> any(). 
execute(Name, Fun) ->
    hottub:execute(Name, Fun).
 
%% @doc Delete `Key' from `Bucket'.
-spec delete(atom(), binary(), binary()) -> ok.
delete(Name, Bucket, Key) ->
    Fun = fun(C) -> riakc_pb_socket:delete(C, Bucket, Key) end,
    execute(Name, Fun).

%% @doc Returns the value associated with `Key' in `Bucket' as `{ok, binary()}'.
%% If an error was encountered or the value was not present, returns
%% `{error, any()}'.
-spec get(atom(), binary(), binary()) -> {ok, binary()} | {error, any()}.
get(Name, Bucket, Key) ->
    Fun =
        fun(C) ->
            case riakc_pb_socket:get(C, Bucket, Key) of
                {ok, O} -> riakc_obj:get_value(O);
                {error, E} -> {error, E}
            end
        end,
    execute(Name, Fun).

%% @doc Returns the list of keys in `Bucket' as `{ok, list()}'. If an error was
%% encountered, returns `{error, any()}'.
-spec list_keys(atom(), binary()) -> {ok, list()} | {error, any()}.
list_keys(Name, Bucket) ->
    Fun = fun(C) -> riakc_pb_socket:list_keys(C, Bucket) end,
    execute(Name, Fun).

%% @doc Associates `Key' with `Value' in `Bucket'. If `Key' already exists in
%% `Bucket', an update will be preformed.
-spec put(atom(), binary(), binary(), binary()) -> ok.
put(Name, Bucket, Key, Value) ->
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
    execute(Name, Fun),
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
        fun() -> start_link(test_db, 8, "localhost", 8081), ok end,
        fun(_) -> stop(test_db), ok end,
        [fun basics/0]}.

basics() ->
    PingFun = fun(C) -> riakc_pb_socket:ping(C) end,
    N0 = <<"_never_exists">>,
    {B, K, V1, V2} = {<<"groceries">>, <<"mine">>, <<"eggs">>, <<"toast">>},
    ?assertEqual(pong, execute(test_db, PingFun)),
    ?assertEqual(ok, delete(test_db, B, K)),
    ?assertMatch({ok, []}, list_keys(test_db, N0)),
    ?assertMatch({error, _}, get(test_db, N0, N0)),
    ?assertEqual(ok, put(test_db, B, K, V1)),
    ?assertEqual(V1, get(test_db, B, K)),
    ?assertEqual(ok, put(test_db, B, K, V2)),
    ?assertEqual(V2, get(test_db, B, K)),
    ?assertEqual({ok, [K]}, list_keys(test_db, B)),
    ?assertEqual(ok, delete(test_db, B, K)),
    ?assertEqual({ok, []}, list_keys(test_db, B)).

-endif.
