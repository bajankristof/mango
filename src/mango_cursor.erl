-module(mango_cursor).

-import(mango_command, [opts/1]).

-export([new/2, new/3, set_opts/2]).
-export([exhaust/1, exhaust/2]).
-export([get_batch/1, get_batch/2]).
-export([get_more/1, get_more/2]).
-export([get_one/1, get_one/2]).
-export([close/1, close/2]).

-include("defaults.hrl").
-include("mango.hrl").

-spec new(
    Connection :: gen_server:server_ref(),
    Source :: bson:document()
) -> mango:cursor().
new(Connection, #{<<"id">> := _, <<"ns">> := _, <<"firstBatch">> := Batch} = Source) ->
    new(Connection, Source, Batch);
new(Connection, #{<<"id">> := _, <<"ns">> := _, <<"nextBatch">> := Batch} = Source) ->
    new(Connection, Source, Batch);
new(Connection, #{<<"cursor">> := #{<<"id">> := _, <<"ns">> := _} = Source}) ->
    new(Connection, Source).

-spec new(
    Connection :: gen_server:server_ref(),
    Source :: bson:document(),
    Batch :: [bson:document()]
) -> mango:cursor().
new(Connection, #{<<"id">> := Id, <<"ns">> := Namespace}, Batch) ->
    [Database, Collection] = binary:split(Namespace, <<".">>),
    #cursor{id = Id, connection = Connection, database = Database, collection = Collection, batch = Batch}.

-spec set_opts(
    Cursor :: mango:cursor(),
    Opts :: list() | map()
) -> mango:cursor().
set_opts(#cursor{} = Cursor, Opts) ->
    Cursor#cursor{opts = opts(Opts)}.

%% @equiv exhaust(Cursor, ?TIMEOUT)
exhaust(Cursor) ->
    exhaust(Cursor, ?TIMEOUT).

-spec exhaust(
    Cursor :: mango:cursor(),
    Timeout :: timeout()
) -> {ok, [bson:document()]} | {error, term()}.
exhaust(#cursor{batch = Acc0} = Cursor, Timeout) ->
    bson:loop(fun (Acc) ->
        case get_batch(Cursor, Timeout) of
            {nofin, Batch} ->
                {true, Acc ++ Batch};
            {fin, Batch} ->
                {false, {ok, Acc ++ Batch}};
            {error, Reason} ->
                {false, {error, Reason}}
        end
    end, Acc0).

%% @equiv get_more(Cursor, ?TIMEOUT)
get_batch(Cursor) ->
    get_more(Cursor, ?TIMEOUT).

%% @equiv get_more(Cursor, Timeout)
get_batch(Cursor, Timeout) ->
    get_more(Cursor, Timeout).

%% @equiv get_more(Cursor, ?TIMEOUT)
get_more(Cursor) ->
    get_more(Cursor, ?TIMEOUT).

-spec get_more(
    Cursor :: mango:cursor(),
    Timeout :: timeout()
) -> {fin | nofin, [bson:document()]} | {error, term()}.
get_more(#cursor{id = 0}, _) -> {fin, []};
get_more(#cursor{connection = Connection, opts = Opts} = Cursor, Timeout) ->
    Command = mango_command:get_more(Cursor, Opts),
    case mango_connection:run_command(Connection, Command, Timeout) of
        {ok, #cursor{id = 0, batch = Batch}} ->
            {fin, Batch};
        {ok, #cursor{batch = Batch}} ->
            {nofin, Batch};
        {error, Reason} ->
            {error, Reason}
    end.

%% @equiv get_one(Cursor, ?TIMEOUT)
get_one(Cursor) ->
    get_one(Cursor, ?TIMEOUT).

-spec get_one(
    Cursor :: mango:cursor(),
    Timeout :: timeout()
) -> {fin | nofin, undefined | bson:document()} | {error, term()}.
get_one(#cursor{opts = Opts0} = Cursor, Timeout) ->
    Opts = [{<<"batchSize">>, 1} | lists:keydelete(<<"batchSize">>, 1, Opts0)],
    case get_more(Cursor#cursor{opts = Opts}, Timeout) of
        {error, Reason} -> {error, Reason};
        {Statement, [Document]} -> {Statement, Document};
        {_, []} -> {fin, undefined}
    end.

%% @equiv close(Cursor, ?TIMEOUT)
close(Cursor) ->
    close(Cursor, ?TIMEOUT).

-spec close(
    Cursor :: mango:cursor(),
    Timeout :: timeout()
) -> ok | {error, term()}.
close(#cursor{id = 0}, _) -> ok;
close(#cursor{connection = Connection, opts = Opts} = Cursor, Timeout) ->
    Command = mango_command:kill_cursor(Cursor, Opts),
    case mango_connection:run_command(Connection, Command, Timeout) of
        {error, Reason} -> {error, Reason};
        {ok, _} -> ok
    end.
