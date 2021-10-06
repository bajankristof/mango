-module(mango).

-export([aggregate/3, aggregate/4, aggregate/5]).
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

%% @equiv aggregate(Connection, mango_connection:database(Connection), Collection, Pipeline, Opts)
aggregate(Connection, Collection, Pipeline, Opts) ->
    Database = mango_connection:database(Connection),
    aggregate(Connection, Database, Collection, Pipeline, Opts).

-spec aggregate(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Pipeline :: bson:array(),
    Opts :: list() | map()
) -> {ok, cursor()} | {error, term()}.
aggregate(Connection, Database, Collection, Pipeline, Opts) when erlang:is_map(Opts) ->
    aggregate(Connection, Database, Collection, Pipeline, maps:to_list(Opts));
aggregate(Connection, Database, Collection, Pipeline, Opts) ->
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

%% @equiv count(Connection, mango_connection:database(Connection), Collection, Selector, Opts)
count(Connection, Collection, Selector, Opts) ->
    Database = mango_connection:database(Connection),
    count(Connection, Database, Collection, Selector, Opts).

-spec count(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list() | map()
) -> {ok, integer()} | {error, term()}.
count(Connection, Database, Collection, Selector, Opts) when erlang:is_map(Opts) ->
    count(Connection, Database, Collection, Selector, maps:to_list(Opts));
count(Connection, Database, Collection, Selector, Opts) ->
    case mango_command:count(Connection, Database, Collection,
        [{"query", Selector} | Opts]) of
            {ok, #{<<"n">> := Count}} -> {ok, Count};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv delete(Connection, Collection, Selector, 0)
delete(Connection, Collection, Selector) ->
    delete(Connection, Collection, Selector, 0).

%% @equiv delete(Connection, Collection, Selector, Limit, [])
delete(Connection, Collection, Selector, Limit) ->
    delete(Connection, Collection, Selector, Limit, []).

%% @equiv delete(Connection, mango_connection:database(Connection), Collection, Selector, Limit, Opts)
delete(Connection, Collection, Selector, Limit, Opts) ->
    delete(Connection, mango_connection:database(Connection), Collection, Selector, Limit, Opts).

-spec delete(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Limit :: integer(),
    Opts :: list() | map()
) -> {ok, bson:document()} | {error, term()}.
delete(Connection, Database, Collection, Selector, Limit, Opts) when erlang:is_map(Opts) ->
    delete(Connection, Database, Collection, Selector, Limit, maps:to_list(Opts));
delete(Connection, Database, Collection, Selector, Limit, Opts) ->
    Statement = maps:from_list([{"q", Selector}, {"limit", Limit} | Opts]),
    mango_command:delete(Connection, Database, Collection, [Statement], []).

%% @equiv distinct(Connection, Collection, Key, #{})
distinct(Connection, Collection, Key) ->
    distinct(Connection, Collection, Key, #{}).

%% @equiv distinct(Connection, Collection, Key, Selector, [])
distinct(Connection, Collection, Key, Selector) ->
    distinct(Connection, Collection, Key, Selector, []).

%% @equiv distinct(Connection, mango_connection:database(Connection), Collection, Key, Selector, Opts)
distinct(Connection, Collection, Key, Selector, Opts) ->
    distinct(Connection, mango_connection:database(Connection), Collection, Key, Selector, Opts).

-spec distinct(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Key :: atom() | binary() | list(),
    Selector :: bson:document(),
    Opts :: list() | map()
) -> {ok, list()} | {error, term()}.
distinct(Connection, Database, Collection, Key, Selector, Opts) when erlang:is_map(Opts) ->
    distinct(Connection, Database, Collection, Key, Selector, maps:to_list(Opts));
distinct(Connection, Database, Collection, Key, Selector, Opts) ->
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

%% @equiv find(Connection, mango_connection:database(Connection), Collection, Selector, Opts)
find(Connection, Collection, Selector, Opts) ->
    find(Connection, mango_connection:database(Connection), Collection, Selector, Opts).

-spec find(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list() | map()
) -> {ok, cursor()} | {error, term()}.
find(Connection, Database, Collection, Selector, Opts) when erlang:is_map(Opts) ->
    find(Connection, Database, Collection, Selector, maps:to_list(Opts));
find(Connection, Database, Collection, Selector, Opts) ->
    case mango_command:find(Connection, Database, Collection,
        [{"filter", Selector}, {"batchSize", 0}, {"singleBatch", false} | Opts]) of
            {ok, #{<<"cursor">> := Spec}} ->
                {ok, maps:without([<<"firstBatch">>], Spec)};
            {error, Reason} -> {error, Reason}
    end.

%% @equiv find_and_remove(Connection, Collection, Selector, [])
find_and_remove(Connection, Collection, Selector) ->
    find_and_remove(Connection, Collection, Selector, []).

%% @equiv find_and_remove(Connection, mango_connection:database(Connection), Collection, Selector, Opts)
find_and_remove(Connection, Collection, Selector, Opts) ->
    find_and_remove(Connection, mango_connection:database(Connection), Collection, Selector, Opts).

-spec find_and_remove(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list() | map()
) -> bson:document() | undefined | {error, term()}.
find_and_remove(Connection, Database, Collection, Selector, Opts) when erlang:is_map(Opts) ->
    find_and_remove(Connection, Database, Collection, Selector, maps:to_list(Opts));
find_and_remove(Connection, Database, Collection, Selector, Opts) ->
    case mango_command:find_and_modify(Connection, Database, Collection,
        [{"query", Selector}, {"remove", true} | Opts]) of
            {ok, #{<<"value">> := null}} -> undefined;
            {ok, #{<<"value">> := Document}} -> Document;
            {ok, #{}} -> undefined;
            {error, Reason} -> {error, Reason}
    end.

%% @equiv find_and_update(Connection, Collection, Selector, Update, [])
find_and_update(Connection, Collection, Selector, Update) ->
    find_and_update(Connection, Collection, Selector, Update, []).

%% @equiv find_and_update(Connection, mango_connection:database(Connection), Collection, Selector, Update, Opts)
find_and_update(Connection, Collection, Selector, Update, Opts) ->
    find_and_update(Connection, mango_connection:database(Connection), Collection, Selector, Update, Opts).

-spec find_and_update(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Update :: bson:document(),
    Opts :: list() | map()
) -> bson:document() | undefined | {error, term()}.
find_and_update(Connection, Database, Collection, Selector, Update, Opts) when erlang:is_map(Opts) ->
    find_and_update(Connection, Database, Collection, Selector, Update, maps:to_list(Opts));
find_and_update(Connection, Database, Collection, Selector, Update, Opts) ->
    case mango_command:find_and_modify(Connection, Database, Collection,
        [{"query", Selector}, {"update", Update} | Opts]) of
            {ok, #{<<"value">> := null}} -> undefined;
            {ok, #{<<"value">> := Document}} -> Document;
            {ok, #{}} -> undefined;
            {error, Reason} -> {error, Reason}
    end.

%% @equiv find_one(Connection, Collection, #{})
find_one(Connection, Collection) ->
    find_one(Connection, Collection, #{}).

%% @equiv find_one(Connection, Collection, Selector, [])
find_one(Connection, Collection, Selector) ->
    find_one(Connection, Collection, Selector, []).

%% @equiv find_one(Connection, mango_connection:database(Connection), Collection, Selector, Opts)
find_one(Connection, Collection, Selector, Opts) ->
    find_one(Connection, mango_connection:database(Connection), Collection, Selector, Opts).

-spec find_one(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list() | map()
) -> bson:document() | undefined | {error, term()}.
find_one(Connection, Database, Collection, Selector, Opts) when erlang:is_map(Opts) ->
    find_one(Connection, Database, Collection, Selector, maps:to_list(Opts));
find_one(Connection, Database, Collection, Selector, Opts) ->
    case mango_command:find(Connection, Database, Collection,
        [{"filter", Selector}, {"batchSize", 1}, {"singleBatch", true} | Opts]) of
            {ok, #{<<"cursor">> := #{<<"firstBatch">> := [Document]}}} -> Document;
            {ok, #{<<"cursor">> := #{<<"firstBatch">> := []}}} -> undefined;
            {error, Reason} -> {error, Reason}
    end.

%% @equiv insert(Connection, Collection, Statement, [])
insert(Connection, Collection, Statement) ->
    insert(Connection, Collection, Statement, []).

%% @equiv insert(Connection, mango_connection:database(Connection), Collection, Statement, Opts)
insert(Connection, Collection, Statement, Opts) ->
    insert(Connection, mango_connection:database(Connection), Collection, Statement, Opts).

-spec insert(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    What :: bson:document() | [bson:document()],
    Opts :: list() | map()
) -> {ok, bson:document()} | {error, term()}.
insert(Connection, Database, Collection, #{} = Statement, Opts) when erlang:is_map(Opts) ->
    insert(Connection, Database, Collection, #{} = Statement, maps:to_list(Opts));
insert(Connection, Database, Collection, #{} = Statement, Opts) ->
    insert(Connection, Database, Collection, [Statement], Opts);
insert(Connection, Database, Collection, Statement, Opts) ->
    mango_command:insert(Connection, Database, Collection, Statement, Opts).

%% @equiv update(Connection, Collection, Selector, Update, false, false)
update(Connection, Collection, Selector, Update) ->
    update(Connection, Collection, Selector, Update, false, false).

%% @equiv update(Connection, Collection, Selector, Update, Upsert, Multi, [])
update(Connection, Collection, Selector, Update, Upsert, Multi) ->
    update(Connection, Collection, Selector, Update, Upsert, Multi, []).

%% @equiv update(Connection, mango_connection:database(Connection), Collection, Selector, Update, Upsert, Multi, Opts)
update(Connection, Collection, Selector, Update, Upsert, Multi, Opts) ->
    Database = mango_connection:database(Connection),
    update(Connection, Database, Collection, Selector, Update, Upsert, Multi, Opts).

-spec update(
    Connection :: connection(),
    Database :: database(),
    Collection :: collection(),
    Selector :: bson:document(),
    Update :: bson:document(),
    Upsert :: boolean(),
    Multi :: boolean(),
    Opts :: list() | map()
) -> {ok, bson:document()} | {error, term()}.
update(Connection, Database, Collection, Selector, Update, Upsert, Multi, Opts) when erlang:is_map(Opts) ->
    update(Connection, Database, Collection, Selector, Update, Upsert, Multi, maps:to_list(Opts));
update(Connection, Database, Collection, Selector, Update, Upsert, Multi, Opts) ->
    Statement = maps:from_list([{"q", Selector}, {"u", Update}, {"upsert", Upsert}, {"multi", Multi} | Opts]),
    mango_command:update(Connection, Database, Collection, [Statement], []).

% @equiv run_command(Connection, mango_connection:database(Connection), Command)
run_command(Connection, Command) ->
    Database = mango_connection:database(Connection),
    run_command(Connection, Database, Command).

-spec run_command(
    Connection :: connection(),
    Database :: database(),
    Command :: mango_command:t()
) -> mango_op_msg:response().
run_command(Connection, Database, Command) ->
    mango_command:run(Connection, Database, Command).
