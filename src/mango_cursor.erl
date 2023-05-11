-module(mango_cursor).

-import(mango_command, [opts/1]).

-export([new/2, set_opts/2]).
-export([exhaust/1, exhaust/2]).
-export([get_batch/1, get_batch/2]).
-export([get_more/1, get_more/2]).
-export([get_one/1, get_one/2]).
-export([close/1, close/2]).

-include("mango.hrl").
-include("defaults.hrl").

-spec new(
    Connection :: mango:connection(),
    Source :: bson:document()
) -> mango:cursor().
new(Connection, #{<<"id">> := Id, <<"ns">> := Namespace, <<"firstBatch">> := Batch}) ->
    [Database, Collection] = binary:split(Namespace, <<".">>),
    #'mango.cursor'{id = Id, connection = Connection, database = Database, collection = Collection, first_batch = Batch};
new(Connection, #{<<"cursor">> := #{<<"id">> := _, <<"ns">> := _, <<"firstBatch">> := _} = Source}) ->
    new(Connection, Source).

-spec set_opts(
    Cursor :: mango:cursor(),
    Opts :: list() | map()
) -> mango:cursor().
set_opts(#'mango.cursor'{} = Cursor, Opts) ->
    Cursor#'mango.cursor'{opts = opts(Opts)}.

%% @equiv exhaust(Cursor, ?DEFAULT_TIMEOUT)
exhaust(Cursor) ->
    exhaust(Cursor, ?DEFAULT_TIMEOUT).

-spec exhaust(
    Cursor :: mango:cursor(),
    Timeout :: timeout()
) -> {ok, [bson:document()]} | {error, term()}.
exhaust(#'mango.cursor'{first_batch = Acc0} = Cursor, Timeout) ->
    bson:loop(fun (Acc) ->
        case get_batch(Cursor, Timeout) of
            {nofin, Documents} ->
                {true, Acc ++ Documents};
            {fin, Documents} ->
                {false, {ok, Acc ++ Documents}};
            {error, Reason} ->
                {false, {error, Reason}}
        end
    end, Acc0).

%% @equiv get_more(Cursor, ?DEFAULT_TIMEOUT)
get_batch(Cursor) ->
    get_more(Cursor, ?DEFAULT_TIMEOUT).

%% @equiv get_more(Cursor, Timeout)
get_batch(Cursor, Timeout) ->
    get_more(Cursor, Timeout).

%% @equiv get_more(Cursor, ?DEFAULT_TIMEOUT)
get_more(Cursor) ->
    get_more(Cursor, ?DEFAULT_TIMEOUT).

-spec get_more(
    Cursor :: mango:cursor(),
    Timeout :: timeout()
) -> {fin | nofin, [bson:document()]} | {error, term()}.
get_more(#'mango.cursor'{id = 0}, _) -> {fin, []};
get_more(#'mango.cursor'{} = Cursor, Timeout) ->
    Command = mango_command:get_more(Cursor, Cursor#'mango.cursor'.opts),
    case mango_topology:command(Cursor#'mango.cursor'.connection, Command, Timeout) of
        {ok, #{<<"cursor">> := #{<<"id">> := 0, <<"nextBatch">> := Documents}}} ->
            {fin, Documents};
        {ok, #{<<"cursor">> := #{<<"nextBatch">> := Documents}}} ->
            {nofin, Documents};
        {error, Reason} ->
            {error, Reason}
    end.

%% @equiv get_one(Cursor, ?DEFAULT_TIMEOUT)
get_one(Cursor) ->
    get_one(Cursor, ?DEFAULT_TIMEOUT).

-spec get_one(
    Cursor :: mango:cursor(),
    Timeout :: timeout()
) -> {fin | nofin, undefined | bson:document()} | {error, term()}.
get_one(Cursor, Timeout) ->
    Opts = [{<<"batchSize">>, 1} | Cursor#'mango.cursor'.opts],
    case get_more(Cursor#'mango.cursor'{opts = Opts}, Timeout) of
        {error, Reason} -> {error, Reason};
        {Statement, [Document]} -> {Statement, Document};
        {_, []} -> {fin, undefined}
    end.

%% @equiv close(Cursor, ?DEFAULT_TIMEOUT)
close(Cursor) ->
    close(Cursor, ?DEFAULT_TIMEOUT).

-spec close(
    Cursor :: mango:cursor(),
    Timeout :: timeout()
) -> ok | {error, term()}.
close(#'mango.cursor'{id = 0}, _) -> ok;
close(#'mango.cursor'{connection = Connection} = Cursor, Timeout) ->
    Command = mango_command:kill_cursor(Cursor, Cursor#'mango.cursor'.opts),
    case mango_topology:command(Connection, Command, Timeout) of
        {error, Reason} -> {error, Reason};
        {ok, _} -> ok
    end.
