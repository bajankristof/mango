-module(mango_cursor).

-export([exhaust/2, exhaust/3]).
-export([get_batch/2, get_batch/3]).
-export([get_more/2, get_more/3]).
-export([get_one/2, get_one/3]).
-export([close/2, close/3]).

%% @equiv exhaust(Connection, Cursor, [])
exhaust(Connection, Cursor) ->
    exhaust(Connection, Cursor, []).

-spec exhaust(
    Connection :: mango:connection(),
    Cursor :: mango:cursor(),
    Opts :: list() | map()
) -> {ok, [bson:document()]} | {error, term()}.
exhaust(Connection, Cursor, Opts) when erlang:is_map(Opts) ->
    exhaust(Connection, Cursor, maps:to_list(Opts));
exhaust(Connection, Cursor, Opts) ->
    bson:loop(fun (Acc) ->
        case get_batch(Connection, Cursor, Opts) of
            {nofin, Documents} -> {true, Acc ++ Documents};
            {fin, Documents} -> {false, {ok, Acc ++ Documents}};
            {error, Reason} -> {false, {error, Reason}}
        end
    end, []).

%% @equiv get_more(Connection, Cursor)
get_batch(Connection, Cursor) ->
    get_more(Connection, Cursor).

%% @equiv get_more(Connection, Cursor, Opts)
get_batch(Connection, Cursor, Opts) ->
    get_more(Connection, Cursor, Opts).

%% @equiv get_more(Connection, Cursor, [])
get_more(Connection, Cursor) ->
    get_more(Connection, Cursor, []).

-spec get_more(
    Connection :: mango:connection(),
    Cursor :: mango:cursor(),
    Opts :: list() | map()
) -> {fin | nofin, [bson:document()]} | {error, term()}.
get_more(_, #{<<"id">> := 0}, _) -> {fin, []};
get_more(Connection, Cursor, Opts) when erlang:is_map(Opts) ->
    get_more(Connection, Cursor, maps:to_list(Opts));
get_more(Connection, Cursor, Opts) ->
    case mango_command:get_more(Connection, Cursor, Opts) of
        {ok, #{<<"cursor">> := #{<<"id">> := 0, <<"nextBatch">> := Documents}}} -> {fin, Documents};
        {ok, #{<<"cursor">> := #{<<"nextBatch">> := Documents}}} -> {nofin, Documents};
        {error, Reason} -> {error, Reason}
    end.

%% @equiv get_one(Connection, Cursor, [])
get_one(Connection, Cursor) ->
    get_one(Connection, Cursor, []).

-spec get_one(
    Connection :: mango:connection(),
    Cursor :: mango:cursor(),
    Opts :: list() | map()
) -> {fin | nofin, undefined | bson:document()} | {error, term()}.
get_one(Connection, Cursor, Opts) when erlang:is_map(Opts) ->
    get_one(Connection, Cursor, maps:to_list(Opts));
get_one(Connection, Cursor, Opts) ->
    case get_more(Connection, Cursor, [{"batchSize", 1} | Opts]) of
        {error, Reason} -> {error, Reason};
        {Statement, [Document]} -> {Statement, Document};
        {_, []} -> {fin, undefined}
    end.

%% @equiv close(Connection, Cursor, [])
close(Connection, Cursor) ->
    close(Connection, Cursor, []).

-spec close(
    Connection :: mango:connection(),
    Cursor :: mango:cursor(),
    Opts :: list()
) -> ok | {error, term()}.
close(_, #{<<"id">> := 0}, _) -> ok;
close(Connection, Cursor, Opts) ->
    case mango_command:kill_cursor(Connection, Cursor, Opts) of
        {error, Reason} -> {error, Reason};
        {ok, _} -> ok
    end.
