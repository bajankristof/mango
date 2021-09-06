-module(mango).

-export([aggregate/3, aggregate/4]).
-export([count/2, count/3, count/4]).
-export([distinct/3, distinct/4, distinct/5]).
-export([find/2, find/3, find/4]).
-export([find_and_remove/3, find_and_remove/4]).
-export([find_and_update/4, find_and_update/5]).
-export([find_one/2, find_one/3, find_one/4]).
-export([insert/3, insert/4]).
-export([update/3, update/4]).
-export([run_command/2]).

-type connection() :: pid() | atom() | {via, atom(), term()}.
-type database() :: atom() | binary().
-type collection() :: atom() | binary().
-type namespace() :: binary().
-type cursor() :: bson:document().
-export_type([
    connection/0,
    database/0,
    collection/0,
    namespace/0,
    cursor/0
]).

%% === Aggregation Functions ===

%% @equiv aggregate(Connection, Collection, Pipeline, [])
aggregate(Connection, Collection, Pipeline) ->
    aggregate(Connection, Collection, Pipeline, []).

-spec aggregate(
    Connection :: connection(),
    Collection :: collection(),
    Pipeline :: bson:array(),
    Opts :: list()
) -> {ok, cursor()} | {error, term()}.
aggregate(Connection, Collection, Pipeline, Opts) ->
    Database = mango_connection:database(Connection),
    case mango_command:aggregate(Connection, Database, Collection, Pipeline,
        [{"cursor", #{"batchSize" => 0}} | Opts]) of
            {ok, #{<<"cursor">> := Spec}} ->
                {ok, maps:without([<<"firstBatch">>], Spec)};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv count(Connection, Collection, #{})
count(Connection, Collection) ->
    count(Connection, Collection, #{}).

%% @equiv count(Connection, Collection, Selector, [])
count(Connection, Collection, Selector) ->
    count(Connection, Collection, Selector, []).

-spec count(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, integer()} | {error, term()}.
count(Connection, Collection, Selector, Opts) ->
    Database = mango_connection:database(Connection),
    case mango_command:count(Connection, Database, Collection,
        [{"query", Selector} | Opts]) of
            {ok, #{<<"n">> := Count}} -> {ok, Count};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv distinct(Connection, Collection, Key, #{})
distinct(Connection, Collection, Key) ->
    distinct(Connection, Collection, Key, #{}).

%% @equiv distinct(Connection, Collection, Key, Selector, [])
distinct(Connection, Collection, Key, Selector) ->
    distinct(Connection, Collection, Key, Selector, []).

-spec distinct(
    Connection :: connection(),
    Collection :: collection(),
    Key :: atom() | binary() | list(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, list()} | {error, term()}.
distinct(Connection, Collection, Key, Selector, Opts) ->
    Database = mango_connection:database(Connection),
    case mango_command:distinct(Connection, Database, Collection, Key,
        [{"query", Selector} | Opts]) of
            {ok, #{<<"values">> := Values}} -> {ok, Values};
            {error, Reason} -> {error, Reason}
    end.

%% === Query and Write Operation Functions ===

%% @equiv find(Connection, Collection, #{})
find(Connection, Collection) ->
    find(Connection, Collection, #{}).

%% @equiv find(Connection, Collection, Selector, [])
find(Connection, Collection, Selector) ->
    find(Connection, Collection, Selector, []).

-spec find(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, cursor()} | {error, term()}.
find(Connection, Collection, Selector, Opts) ->
    Database = mango_connection:database(Connection),
    case mango_command:find(Connection, Database, Collection,
        [{"filter", Selector}, {"batchSize", 0}, {"singleBatch", false} | Opts]) of
            {ok, #{<<"cursor">> := Spec}} ->
                {ok, maps:without([<<"firstBatch">>], Spec)};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv find_and_remove(Connection, Collection, Selector, [])
find_and_remove(Connection, Collection, Selector) ->
    find_and_remove(Connection, Collection, Selector, []).

-spec find_and_remove(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, bson:document()} | {error, term()}.
find_and_remove(Connection, Collection, Selector, Opts) ->
    Database = mango_connection:database(Connection),
    mango_command:find_and_modify(Connection, Database, Collection,
        [{"query", Selector}, {"remove", true} | Opts]).

%% @equiv find_and_update(Connection, Collection, Selector, Statement, [])
find_and_update(Connection, Collection, Selector, Statement) ->
    find_and_update(Connection, Collection, Selector, Statement, []).

-spec find_and_update(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Statement :: bson:document(),
    Opts :: list()
) -> {ok, bson:document()} | {error, term()}.
find_and_update(Connection, Collection, Selector, Statement, Opts) ->
    Database = mango_connection:database(Connection),
    case mango_command:find_and_modify(Connection, Database, Collection,
        [{"query", Selector}, {"update", Statement} | Opts]) of
            {ok, #{<<"value">> := null} = Document} ->
                {ok, maps:without([<<"value">>], Document)};
            {ok, #{<<"value">> := _} = Document} -> {ok, Document};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv find_one(Connection, Collection, #{})
find_one(Connection, Collection) ->
    find_one(Connection, Collection, #{}).

%% @equiv find_one(Connection, Collection, Selector, [])
find_one(Connection, Collection, Selector) ->
    find_one(Connection, Collection, Selector, []).

-spec find_one(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, bson:document()} | {error, term()}.
find_one(Connection, Collection, Selector, Opts) ->
    Database = mango_connection:database(Connection),
    case mango_command:find(Connection, Database, Collection,
        [{"filter", Selector}, {"batchSize", 1}, {"singleBatch", true} | Opts]) of
            {ok, #{<<"cursor">> := #{<<"firstBatch">> := [Document]}}} -> Document;
            {ok, #{<<"cursor">> := #{<<"firstBatch">> := []}}} -> undefined;
            {error, Reason} -> {error, Reason}
    end.

%% @equiv insert(Connection, Collection, Statement, [])
insert(Connection, Collection, Statement) ->
    insert(Connection, Collection, Statement, []).

-spec insert(
    Connection :: connection(),
    Collection :: collection(),
    What :: bson:document() | [bson:document()],
    Opts :: list()
) -> ok | {error, term()}.
insert(Connection, Collection, #{} = Statement, Opts) ->
    insert(Connection, Collection, [Statement], Opts);
insert(Connection, Collection, Statement, Opts) ->
    Database = mango_connection:database(Connection),
    case mango_command:insert(Connection, Database, Collection, Statement, Opts) of
        {ok, #{<<"n">> := Count}} -> {ok, Count};
        {error, Reason} -> {error, Reason}
    end.

%% @equiv update(Connection, Collection, Statement, [])
update(Connection, Collection, Statement) ->
    update(Connection, Collection, Statement, []).

-spec update(
    Connection :: connection(),
    Collection :: collection(),
    Statement :: bson:document() | [bson:document()],
    Opts :: list()
) -> term().
update(Connection, Collection, #{} = Statement, Opts) ->
    update(Connection, Collection, [Statement], Opts);
update(Connection, Collection, Statement, Opts) ->
    Database = mango_connection:database(Connection),
    mango_command:update(Connection, Database, Collection, Statement, Opts).

-spec run_command(
    Connection :: connection(),
    Command :: mango_command:t()
) -> mango_op_msg:response().
run_command(Connection, Command) ->
    Database = mango_connection:database(Connection),
    mango_command:run(Connection, Database, Command).
