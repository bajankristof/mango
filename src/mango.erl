-module(mango).

-import(mango_command, [opts/1]).

-export([start/1, start_link/1, stop/1]).
-export([aggregate/3, aggregate/4]).
-export([count/2, count/3, count/4, count/5]).
-export([delete/3, delete/4, delete/5]).
-export([distinct/3, distinct/4, distinct/5, distinct/6]).
-export([find/2, find/3, find/4, find/5]).
-export([find_and_remove/3, find_and_remove/4, find_and_remove/5]).
-export([find_and_update/4, find_and_update/5, find_and_update/6]).
-export([find_one/2, find_one/3, find_one/4, find_one/5]).
-export([insert/3, insert/4, insert/5]).
-export([update/4, update/6, update/7, update/8]).
-export([run_command/2, run_command/3]).

-include("mango.hrl").
-include("./constants.hrl").

-type connection() :: pid() | atom() | {via, atom(), term()}.
-type database() :: atom() | binary().
-type collection() :: atom() | binary().
-type namespace() :: binary().
-type cursor() :: bson:document().
-type command() :: #'mango.command'{}.
-export_type([
    connection/0,
    database/0,
    collection/0,
    namespace/0,
    cursor/0,
    command/0
]).

%% === API Functions ===

-spec start(Opts :: map() | list()) -> supervisor:on_start().
start(Opts) ->
    mango_connection:start(Opts).

-spec start_link(Opts :: map() | list()) -> supervisor:on_start().
start_link(Opts) ->
    mango_connection:start_link(Opts).

-spec stop(Connection :: connection()) -> ok.
stop(Connection) ->
    mango_connection:stop(Connection).

%% === Aggregation Functions ===

%% @equiv aggregate(Connection, Collection, Pipeline, [])
aggregate(Connection, Collection, Pipeline) ->
    aggregate(Connection, Collection, Pipeline, []).

%% @equiv aggregate(Connection, Collection, Pipeline, Opts, ?DEFAULT_TIMEOUT)
aggregate(Connection, Collection, Pipeline, Opts) ->
    aggregate(Connection, Collection, Pipeline, Opts, ?DEFAULT_TIMEOUT).

-spec aggregate(
    Connection :: connection(),
    Collection :: collection(),
    Pipeline :: bson:array(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> {ok, cursor()} | {error, term()}.
aggregate(Connection, Collection, Pipeline, Opts0, Timeout) ->
    Opts = [{cursor, #{"batchSize" => 0}} | opts(Opts0)],
    Database = mango_connection:database(Connection),
    Command = mango_command:aggregate(Database, Collection, Pipeline, Opts),
    run_command(Connection, Command, Timeout).

%% @equiv count(Connection, Collection, #{})
count(Connection, Collection) ->
    count(Connection, Collection, #{}).

%% @equiv count(Connection, Collection, Selector, [])
count(Connection, Collection, Selector) ->
    count(Connection, Collection, Selector, []).

%% @equiv count(Connection, Collection, Selector, Opts, ?DEFAULT_TIMEOUT)
count(Connection, Collection, Selector, Opts) ->
    count(Connection, Collection, Selector, Opts, ?DEFAULT_TIMEOUT).

-spec count(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> {ok, integer()} | {error, term()}.
count(Connection, Collection, Selector, Opts0, Timeout) ->
    Opts = [{'query', Selector} | opts(Opts0)],
    Database = mango_connection:database(Connection),
    Command = mango_command:count(Database, Collection, Opts),
    case run_command(Connection, Command, Timeout) of
        {ok, #{<<"n">> := Count}} -> {ok, Count};
        {error, Reason} -> {error, Reason}
    end.

%% @equiv delete(Connection, Collection, Selector, 0)
delete(Connection, Collection, Selector) ->
    delete(Connection, Collection, Selector, 0).

%% @equiv delete(Connection, Collection, Selector, Limit, [])
delete(Connection, Collection, Selector, Limit) ->
    delete(Connection, Collection, Selector, Limit, []).

%% @equiv delete(Connection, Collection, Selector, Limit, Opts, ?DEFAULT_TIMEOUT)
delete(Connection, Collection, Selector, Limit, Opts) ->
    delete(Connection, Collection, Selector, Limit, Opts, ?DEFAULT_TIMEOUT).

-spec delete(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Limit :: integer(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> {ok, bson:document()} | {error, term()}.
delete(Connection, Collection, Selector, Limit, Opts, Timeout) ->
    Statement = maps:from_list([{"q", Selector}, {"limit", Limit} | opts(Opts)]),
    Database = mango_connection:database(Connection),
    Command = mango_command:delete(Database, Collection, [Statement], []),
    run_command(Connection, Command, Timeout).

%% @equiv distinct(Connection, Collection, Key, #{})
distinct(Connection, Collection, Key) ->
    distinct(Connection, Collection, Key, #{}).

%% @equiv distinct(Connection, Collection, Key, Selector, [])
distinct(Connection, Collection, Key, Selector) ->
    distinct(Connection, Collection, Key, Selector, []).

%% @equiv distinct(Connection, Collection, Key, Selector, Opts, ?DEFAULT_TIMEOUT)
distinct(Connection, Collection, Key, Selector, Opts) ->
    distinct(Connection, Collection, Key, Selector, Opts, ?DEFAULT_TIMEOUT).

-spec distinct(
    Connection :: connection(),
    Collection :: collection(),
    Key :: atom() | binary() | list(),
    Selector :: bson:document(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> {ok, list()} | {error, term()}.
distinct(Connection, Collection, Key, Selector, Opts0, Timeout) ->
    Opts = [{'query', Selector} | opts(Opts0)],
    Database = mango_connection:database(Connection),
    Command = mango_command:distinct(Database, Collection, Key, Opts),
    case run_command(Connection, Command, Timeout) of
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

%% @equiv find(Connection, Collection, Selector, Opts, ?DEFAULT_TIMEOUT)
find(Connection, Collection, Selector, Opts) ->
    find(Connection, Collection, Selector, Opts, ?DEFAULT_TIMEOUT).

-spec find(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> {ok, cursor()} | {error, term()}.
find(Connection, Collection, Selector, Opts0, Timeout) ->
    Opts = [{"filter", Selector}, {"batchSize", 0}, {"singleBatch", false} | opts(Opts0)],
    Database = mango_connection:database(Connection),
    Command = mango_command:find(Database, Collection, Opts),
    case run_command(Connection, Command, Timeout) of
        {ok, Cursor} -> {ok, Cursor};
        {error, Reason} -> {error, Reason}
    end.

%% @equiv find_and_remove(Connection, Collection, Selector, [])
find_and_remove(Connection, Collection, Selector) ->
    find_and_remove(Connection, Collection, Selector, []).

%% @equiv find_and_remove(Connection, Collection, Selector, Opts, ?DEFAULT_TIMEOUT)
find_and_remove(Connection, Collection, Selector, Opts) ->
    find_and_remove(Connection, Collection, Selector, Opts, ?DEFAULT_TIMEOUT).

-spec find_and_remove(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> bson:document() | undefined | {error, term()}.
find_and_remove(Connection, Collection, Selector, Opts0, Timeout) ->
    Opts = [{"query", Selector}, {"remove", true} | opts(Opts0)],
    Database = mango_connection:database(Connection),
    Command = mango_command:find_and_modify(Database, Collection, Opts),
    case run_command(Connection, Command, Timeout) of
        {error, Reason} -> {error, Reason};
        {ok, #{<<"value">> := null}} -> undefined;
        {ok, #{<<"value">> := Document}} -> Document;
        {ok, #{}} -> undefined
    end.

%% @equiv find_and_update(Connection, Collection, Selector, Update, [])
find_and_update(Connection, Collection, Selector, Update) ->
    find_and_update(Connection, Collection, Selector, Update, []).

%% @equiv find_and_update(Connection, Collection, Selector, Update, Opts, ?DEFAULT_TIMEOUT)
find_and_update(Connection, Collection, Selector, Update, Opts) ->
    find_and_update(Connection, Collection, Selector, Update, Opts, ?DEFAULT_TIMEOUT).

-spec find_and_update(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Update :: bson:document(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> bson:document() | undefined | {error, term()}.
find_and_update(Connection, Collection, Selector, Update, Opts0, Timeout) ->
    Opts = [{"query", Selector}, {"update", Update} | opts(Opts0)],
    Database = mango_connection:database(Connection),
    Command = mango_command:find_and_modify(Database, Collection, Opts),
    case run_command(Connection, Command, Timeout) of
        {error, Reason} -> {error, Reason};
        {ok, #{<<"value">> := null}} -> undefined;
        {ok, #{<<"value">> := Document}} -> Document;
        {ok, #{}} -> undefined
    end.

%% @equiv find_one(Connection, Collection, #{})
find_one(Connection, Collection) ->
    find_one(Connection, Collection, #{}).

%% @equiv find_one(Connection, Collection, Selector, [])
find_one(Connection, Collection, Selector) ->
    find_one(Connection, Collection, Selector, []).

%% @equiv find_one(Connection, Collection, Selector, Opts, ?DEFAULT_TIMEOUT)
find_one(Connection, Collection, Selector, Opts) ->
    find_one(Connection, Collection, Selector, Opts, ?DEFAULT_TIMEOUT).

-spec find_one(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> bson:document() | undefined | {error, term()}.
find_one(Connection, Collection, Selector, Opts0, Timeout) ->
    Opts = [{"filter", Selector}, {"batchSize", 1}, {"singleBatch", true} | opts(Opts0)],
    Database = mango_connection:database(Connection),
    Command = mango_command:find(Database, Collection, Opts),
    case run_command(Connection, Command, Timeout) of
        {ok, #{<<"cursor">> := #{<<"firstBatch">> := [Document]}}} -> Document;
        {ok, #{<<"cursor">> := #{<<"firstBatch">> := []}}} -> undefined;
        {error, Reason} -> {error, Reason}
    end.

%% @equiv insert(Connection, Collection, Statement, [])
insert(Connection, Collection, Statement) ->
    insert(Connection, Collection, Statement, []).

%% @equiv insert(Connection, Collection, Statement, Opts, ?DEFAULT_TIMEOUT)
insert(Connection, Collection, Statement, Opts) ->
    insert(Connection, Collection, Statement, Opts, ?DEFAULT_TIMEOUT).

-spec insert(
    Connection :: connection(),
    Collection :: collection(),
    What :: bson:document() | [bson:document()],
    Timeout :: timeout(),
    Opts :: list() | map()
) -> {ok, bson:document()} | {error, term()}.
insert(Connection, Collection, #{} = Statement, Opts, Timeout) ->
    insert(Connection, Collection, [Statement], Opts, Timeout);
insert(Connection, Collection, Statement, Opts, Timeout) ->
    Database = mango_connection:database(Connection),
    Command = mango_command:insert(Database, Collection, Statement, Opts),
    run_command(Connection, Command, Timeout).

%% @equiv update(Connection, Collection, Selector, Update, false, false)
update(Connection, Collection, Selector, Update) ->
    update(Connection, Collection, Selector, Update, false, false).

%% @equiv update(Connection, Collection, Selector, Update, Upsert, Multi, [])
update(Connection, Collection, Selector, Update, Upsert, Multi) ->
    update(Connection, Collection, Selector, Update, Upsert, Multi, []).

%% @equiv update(Connection, Collection, Selector, Update, Upsert, Multi, Opts, ?DEFAULT_TIMEOUT)
update(Connection, Collection, Selector, Update, Upsert, Multi, Opts) ->
    update(Connection, Collection, Selector, Update, Upsert, Multi, Opts, ?DEFAULT_TIMEOUT).

-spec update(
    Connection :: connection(),
    Collection :: collection(),
    Selector :: bson:document(),
    Update :: bson:document(),
    Upsert :: boolean(),
    Multi :: boolean(),
    Timeout :: timeout(),
    Opts :: list() | map()
) -> {ok, bson:document()} | {error, term()}.
update(Connection, Collection, Selector, Update, Upsert, Multi, Opts, Timeout) ->
    Database = mango_connection:database(Connection),
    Statement = maps:from_list([{"q", Selector}, {"u", Update}, {"upsert", Upsert}, {"multi", Multi} | opts(Opts)]),
    Command = mango_command:update(Database, Collection, [Statement], []),
    run_command(Connection, Command, Timeout).

% @equiv run_command(Connection, Command, ?DEFAULT_TIMEOUT)
run_command(Connection, Command) ->
    run_command(Connection, Command, ?DEFAULT_TIMEOUT).

-spec run_command(
    Connection :: connection(),
    Command :: command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
run_command(Connection, #'mango.command'{} = Command, Timeout) ->
    mango_connection:command(Connection, Command, Timeout).
