-module(mango).

-export([aggregate/4, aggregate/5]).
-export([count/3, count/4, count/5]).
-export([distinct/4, distinct/5, distinct/6]).
-export([find/3, find/4, find/5]).
-export([find_and_remove/4, find_and_remove/5]).
-export([find_and_update/5, find_and_update/6]).
-export([find_one/3, find_one/4, find_one/5]).
-export([insert/4, insert/5]).
-export([update/4, update/5]).
-export([run_command/3]).

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

%% @equiv aggregate(Connection, Database, Collection, Pipeline)
aggregate(Connection, Database, Collection, Pipeline) ->
    aggregate(Connection, Database, Collection, Pipeline, []).

-spec aggregate(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Pipeline :: bson:array(),
    Opts :: list()
) -> {ok, cursor()} | {error, term()}.
aggregate(Connection, Database, Collection, Pipeline, Opts) ->
    case mango_command:aggregate(Connection, Database, Collection, Pipeline,
        [{"cursor", #{"batchSize" => 0}} | Opts]) of
            {ok, #{<<"cursor">> := Spec}} ->
                {ok, maps:without([<<"firstBatch">>], Spec)};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv count(Connection, Database, Collection, #{})
count(Connection, Database, Collection) ->
    count(Connection, Database, Collection, #{}).

%% @equiv count(Connection, Database, Collection, Selector, [])
count(Connection, Database, Collection, Selector) ->
    count(Connection, Database, Collection, Selector, []).

-spec count(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, integer()} | {error, term()}.
count(Connection, Database, Collection, Selector, Opts) ->
    case mango_command:count(Connection, Database, Collection,
        [{"query", Selector} | Opts]) of
            {ok, #{<<"n">> := Count}} -> {ok, Count};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv distinct(Connection, Database, Collection, Key, #{})
distinct(Connection, Database, Collection, Key) ->
    distinct(Connection, Database, Collection, Key, #{}).

%% @equiv distinct(Connection, Database, Collection, Key, Selector, [])
distinct(Connection, Database, Collection, Key, Selector) ->
    distinct(Connection, Database, Collection, Key, Selector, []).

-spec distinct(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Key :: atom() | binary() | list(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, list()} | {error, term()}.
distinct(Connection, Database, Collection, Key, Selector, Opts) ->
    case mango_command:distinct(Connection, Database, Collection, Key,
        [{"query", Selector} | Opts]) of
            {ok, #{<<"values">> := Values}} -> {ok, Values};
            {error, Reason} -> {error, Reason}
    end.

%% === Query and Write Operation Functions ===

%% @equiv find(Connection, Database, Collection, #{})
find(Connection, Database, Collection) ->
    find(Connection, Database, Collection, #{}).

%% @equiv find(Connection, Database, Collection, Selector, [])
find(Connection, Database, Collection, Selector) ->
    find(Connection, Database, Collection, Selector, []).

-spec find(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, cursor()} | {error, term()}.
find(Connection, Database, Collection, Selector, Opts) ->
    case mango_command:find(Connection, Database, Collection,
        [{"filter", Selector}, {"batchSize", 0}, {"singleBatch", false} | Opts]) of
            {ok, #{<<"cursor">> := Spec}} ->
                {ok, maps:without([<<"firstBatch">>], Spec)};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv find_and_remove(Connection, Database, Collection, Selector, [])
find_and_remove(Connection, Database, Collection, Selector) ->
    find_and_remove(Connection, Database, Collection, Selector, []).

-spec find_and_remove(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, bson:document()} | {error, term()}.
find_and_remove(Connection, Database, Collection, Selector, Opts) ->
    mango_command:find_and_modify(Connection, Database, Collection,
        [{"query", Selector}, {"remove", true} | Opts]).

%% @equiv find_and_update(Connection, Database, Collection, Selector, Statement, [])
find_and_update(Connection, Database, Collection, Selector, Statement) ->
    find_and_update(Connection, Database, Collection, Selector, Statement, []).

-spec find_and_update(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Statement :: bson:document(),
    Opts :: list()
) -> {ok, bson:document()} | {error, term()}.
find_and_update(Connection, Database, Collection, Selector, Statement, Opts) ->
    case mango_command:find_and_modify(Connection, Database, Collection,
        [{"query", Selector}, {"update", Statement} | Opts]) of
            {ok, #{<<"value">> := null} = Document} ->
                {ok, maps:without([<<"value">>], Document)};
            {ok, #{<<"value">> := _} = Document} -> {ok, Document};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv find_one(Connection, Database, Collection, #{})
find_one(Connection, Database, Collection) ->
    find_one(Connection, Database, Collection, #{}).

%% @equiv find_one(Connection, Database, Collection, Selector, [])
find_one(Connection, Database, Collection, Selector) ->
    find_one(Connection, Database, Collection, Selector, []).

-spec find_one(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list()
) -> {ok, bson:document()} | {error, term()}.
find_one(Connection, Database, Collection, Selector, Opts) ->
    case mango_command:find(Connection, Database, Collection,
        [{"filter", Selector}, {"batchSize", 1}, {"singleBatch", true} | Opts]) of
            {ok, #{<<"cursor">> := #{<<"firstBatch">> := [Document]}}} -> Document;
            {ok, #{<<"cursor">> := #{<<"firstBatch">> := []}}} -> undefined;
            {error, Reason} -> {error, Reason}
    end.

%% @equiv insert(Connection, Database, Collection, Statement, [])
insert(Connection, Database, Collection, Statement) ->
    insert(Connection, Database, Collection, Statement, []).

-spec insert(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    What :: bson:document() | [bson:document()],
    Opts :: list()
) -> ok | {error, term()}.
insert(Connection, Database, Collection, #{} = Statement, Opts) ->
    insert(Connection, Database, Collection, [Statement], Opts);
insert(Connection, Database, Collection, Statement, Opts) ->
    case mango_command:insert(Connection, Database, Collection, Statement, Opts) of
        {ok, #{<<"n">> := Count}} -> {ok, Count};
        {error, Reason} -> {error, Reason}
    end.

%% @equiv update(Connection, Database, Collection, Statement, [])
update(Connection, Database, Collection, Statement) ->
    update(Connection, Database, Collection, Statement, []).

-spec update(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Statement :: bson:document() | [bson:document()],
    Opts :: list()
) -> term().
update(Connection, Database, Collection, #{} = Statement, Opts) ->
    update(Connection, Database, Collection, [Statement], Opts);
update(Connection, Database, Collection, Statement, Opts) ->
    mango_command:update(Connection, Database, Collection, Statement, Opts).

-spec run_command(
    Connection :: connection(),
    Database :: database(),
    Command :: mango_command:t()
) -> mango_op_msg:response().
run_command(Connection, Database, Command) ->
    mango_command:run(Connection, Database, Command).
