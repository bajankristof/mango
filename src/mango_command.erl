-module(mango_command).

%% API Functions
-export([
    new/2,
    new/3,
    opts/1
]).
%% Aggregation Commands
%% https://docs.mongodb.com/manual/reference/command/#aggregation-commands
-export([
    aggregate/4,
    count/3,
    distinct/4
]).
%% Query and Write Operation Commands
%% https://docs.mongodb.com/manual/reference/command/#query-and-write-operation-commands
-export([
    delete/4,
    find/3,
    find_and_modify/3,
    get_more/2,
    insert/4,
    update/4
]).
%% Administration Commands
%% https://docs.mongodb.com/manual/reference/command/#administration-commands
-export([
    compact/3,
    create_collection/3,
    create_indexes/4,
    current_op/1,
    drop_collection/3,
    drop_database/2,
    drop_indexes/4,
    kill_cursor/2,
    kill_cursors/4,
    list_collections/2,
    list_databases/1,
    list_indexes/3,
    re_index/3,
    rename_collection/3
]).
%% Diagnostic Commands
%% https://docs.mongodb.com/manual/reference/command/#diagnostic-commands
-export([
    ping/0,
    top/0,
    explain/3
]).

-include_lib("bson/include/bson.hrl").
-include("mango.hrl").

-type t() :: #'mango.command'{}.
-export_type([t/0]).

%% @equiv new(Command, Database, [])
new(Command, Database) ->
    new(Command, Database, []).

-spec new(
    Command :: tuple(),
    Database :: mango:database(),
    Opts :: list() | map()
) -> t().
new(Command, Database, Opts) ->
    #'mango.command'{command = Command, database = Database, opts = opts(Opts)}.

-spec opts(Opts :: list() | map()) -> list().
opts(Opts) when erlang:is_map(Opts) ->
    maps:to_list(Opts);
opts(Opts) when erlang:is_list(Opts) ->
    Opts.

%% === Aggregation Commands ===
%% https://docs.mongodb.com/manual/reference/command/#aggregation-commands

-spec aggregate(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Pipeline :: list(),
    Opts :: list() | map()
) -> t().
aggregate(Database, Collection, Pipeline, Opts) ->
    new({<<"aggregate">>, Collection}, Database, [{<<"pipeline">>, Pipeline} | opts(Opts)]).

-spec count(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: list() | map()
) -> t().
count(Database, Collection, Opts) ->
    new({<<"count">>, Collection}, Database, Opts).

-spec distinct(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Key :: atom() | binary(),
    Opts :: list() | map()
) -> t().
distinct(Database, Collection, Key, Opts) ->
    new({<<"distinct">>, Collection}, Database, [{<<"key">>, Key} | opts(Opts)]).

%% === Query and Write Operation Commands ===
%% https://docs.mongodb.com/manual/reference/command/#query-and-write-operation-commands

-spec delete(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Statements :: [bson:document()],
    Opts :: list() | map()
) -> t().
delete(Database, Collection, Statements, Opts) ->
    new({<<"delete">>, Collection}, Database, [{<<"deletes">>, Statements} | opts(Opts)]).

-spec find(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: list() | map()
) -> t().
find(Database, Collection, Opts) ->
    new({<<"find">>, Collection}, Database, Opts).

-spec find_and_modify(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: list() | map()
) -> t().
find_and_modify(Database, Collection, Opts) ->
    new({<<"findAndModify">>, Collection}, Database, Opts).

-spec get_more(
    Cursor :: mango:cursor() | bson:document(),
    Opts :: list() | map()
) -> t().
get_more(#'mango.cursor'{id = Id, database = Database, collection = Collection}, Opts) ->
    get_more(Database, Collection, Id, Opts);
get_more(#{<<"id">> := Id, <<"ns">> := Namespace}, Opts) ->
    [Database, Collection] = binary:split(Namespace, <<".">>),
    get_more(Database, Collection, Id, Opts);
get_more(#{<<"cursor">> := #{<<"id">> := _, <<"ns">> := _} = Cursor}, Opts) ->
    get_more(Cursor, Opts).

-spec get_more(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Id :: integer(),
    Opts :: list() | map()
) -> t().
get_more(Database, Collection, Id, Opts) ->
    new({<<"getMore">>, #'bson.long'{value = Id}}, Database, [{<<"collection">>, Collection} | opts(Opts)]).

-spec insert(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Documents :: [bson:document()],
    Opts :: list() | map()
) -> t().
insert(Database, Collection, Documents, Opts) ->
    new({<<"insert">>, Collection}, Database, [{<<"documents">>, Documents} | opts(Opts)]).

-spec update(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Statements :: [bson:document()],
    Opts :: list() | map()
) -> t().
update(Database, Collection, Statements, Opts) ->
    new({<<"update">>, Collection}, Database, [{<<"updates">>, Statements} | opts(Opts)]).

%% === Administration Commands ===
%% https://docs.mongodb.com/manual/reference/command/#administration-commands

-spec compact(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: list() | map()
) -> t().
compact(Database, Collection, Opts) ->
    new({<<"compact">>, Collection}, Database, Opts).

-spec create_collection(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: list() | map()
) -> t().
create_collection(Database, Collection, Opts) ->
    new({<<"create">>, Collection}, Database, Opts).

-spec create_indexes(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Specs :: [bson:document()],
    Opts :: list() | map()
) -> t().
create_indexes(Database, Collection, Specs, Opts) ->
    new({<<"createIndexes">>, Collection}, Database, [{<<"indexes">>, Specs} | opts(Opts)]).

-spec current_op(All :: boolean()) -> t().
current_op(true) ->
    new({<<"currentOp">>, 1}, <<"admin">>, [{<<"$all">>, true}]);
current_op(false) ->
    new({<<"currentOp">>, 1}, <<"admin">>, [{<<"$ownOps">>, true}]).

-spec drop_collection(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: list() | map()
) -> t().
drop_collection(Database, Collection, Opts) ->
    new({<<"drop">>, Collection}, Database, Opts).

-spec drop_database(
    Database :: mango:database(),
    Opts :: list() | map()
) -> t().
drop_database(Database, Opts) ->
    new({<<"dropDatabase">>, 1}, Database, Opts).

-spec drop_indexes(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Spec :: integer() | list(),
    Opts :: list() | map()
) -> t().
drop_indexes(Database, Collection, Spec, Opts) ->
    new({<<"dropIndexes">>, Collection}, Database, [{<<"indexes">>, Spec} | opts(Opts)]).

-spec kill_cursor(
    Cursor :: mango:cursor() | bson:document(),
    Opts :: list() | map()
) -> t().
kill_cursor(#'mango.cursor'{id = Id, database = Database, collection = Collection}, Opts) ->
    kill_cursors(Database, Collection, [Id], Opts);
kill_cursor(#{<<"id">> := Id, <<"ns">> := Namespace}, Opts) ->
    [Database, Collection] = binary:split(Namespace, <<".">>),
    kill_cursors(Database, Collection, [Id], Opts);
kill_cursor(#{<<"cursor">> := #{<<"id">> := _, <<"ns">> := _} = Cursor}, Opts) ->
    kill_cursor(Cursor, Opts).

-spec kill_cursors(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Ids :: [integer()],
    Opts :: list() | map()
) -> t().
kill_cursors(Database, Collection, Ids, Opts) ->
    Cursors = lists:map(fun (Id) -> #'bson.long'{value = Id} end, Ids),
    new({<<"killCursors">>, Collection}, Database, [{<<"cursors">>, Cursors} | opts(Opts)]).

-spec list_collections(
    Database :: mango:database(),
    Opts :: list() | map()
) -> t().
list_collections(Database, Opts) ->
    new({<<"listCollections">>, 1}, Database, Opts).

-spec list_databases(Opts :: list() | map()) -> t().
list_databases(Opts) ->
    new({<<"listDatabases">>, 1}, admin, Opts).

-spec list_indexes(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: list() | map()
) -> t().
list_indexes(Database, Collection, Opts) ->
    new({<<"listIndexes">>, Collection}, Database, Opts).

-spec re_index(
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: list() | map()
) -> t().
re_index(Database, Collection, Opts) ->
    new({<<"reIndex">>, Collection}, Database, Opts).

-spec rename_collection(
    Collection :: mango:namespace(),
    To :: mango:namespace(),
    Opts :: list() | map()
) -> t().
rename_collection(Collection, To, Opts) ->
    new({<<"renameCollection">>, Collection}, <<"admin">>, [{<<"to">>, To} | opts(Opts)]).

%% === Diagnostic Commands ===
%% https://docs.mongodb.com/manual/reference/command/#diagnostic-commands

-spec ping() -> t().
ping() ->
    new({<<"ping">>, 1}, <<"admin">>, []).

-spec top() -> t().
top() ->
    new({<<"top">>, 1}, <<"admin">>, []).

-spec explain(
    Database :: mango:database(),
    Command :: bson:document(),
    Opts :: list() | map()
) -> t().
explain(Database, Command, Opts) ->
    new({<<"explain">>, Command}, Database, Opts).
