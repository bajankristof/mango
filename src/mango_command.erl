-module(mango_command).

%% Aggregation Commands
%% https://docs.mongodb.com/manual/reference/command/#aggregation-commands
-export([
    aggregate/5,
    count/4,
    distinct/5
]).
%% Query and Write Operation Commands
%% https://docs.mongodb.com/manual/reference/command/#query-and-write-operation-commands
-export([
    delete/5,
    find/4,
    find_and_modify/4,
    get_more/3,
    insert/5,
    update/5
]).
%% Administration Commands
%% https://docs.mongodb.com/manual/reference/command/#administration-commands
-export([
    compact/4,
    create_collection/4,
    create_indexes/5,
    current_op/2,
    drop_collection/4,
    drop_database/3,
    drop_indexes/5,
    kill_cursor/3,
    kill_cursors/5,
    list_collections/3,
    list_databases/2,
    list_indexes/4,
    re_index/4,
    rename_collection/4
]).
%% Diagnostic Commands
%% https://docs.mongodb.com/manual/reference/command/#diagnostic-commands
-export([
    ping/1,
    ping/2,
    top/1,
    explain/4
]).
%% db.runCommand
%% run(Connection, Database, Command)
%% db.adminCommand
%% run(Connection, admin, Command)
-export([run/3]).

-include_lib("bson/include/bson.hrl").

-type t() :: proplists:proplist().
-export_type([t/0]).

%% === Aggregation Commands ===
%% https://docs.mongodb.com/manual/reference/command/#aggregation-commands

-spec aggregate(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Pipeline :: list(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
aggregate(Connection, Database, Collection, Pipeline, Opts) ->
    run(Connection, Database, [{aggregate, Collection}, {"pipeline", Pipeline} | Opts]).

-spec count(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
count(Connection, Database, Collection, Opts) ->
    run(Connection, Database, [{count, Collection} | Opts]).

-spec distinct(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Key :: atom() | binary(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
distinct(Connection, Database, Collection, Key, Opts) ->
    run(Connection, Database, [{distinct, Collection}, {"key", Key} | Opts]).

%% === Query and Write Operation Commands ===
%% https://docs.mongodb.com/manual/reference/command/#query-and-write-operation-commands

-spec delete(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Statements :: [bson:document()],
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
delete(Connection, Database, Collection, Statements, Opts) ->
    run(Connection, Database, [{delete, Collection}, {"deletes", Statements} | Opts]).

-spec find(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
find(Connection, Database, Collection, Opts) ->
    run(Connection, Database, [{find, Collection} | Opts]).

-spec find_and_modify(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
find_and_modify(Connection, Database, Collection, Opts) ->
    run(Connection, Database, [{"findAndModify", Collection} | Opts]).

-spec get_more(
    Connection :: mango:connection(),
    Cursor :: mango:cursor(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
get_more(Connection, #{<<"id">> := Id, <<"ns">> := Namespace}, Opts) ->
    [Database, Collection] = binary:split(Namespace, <<".">>),
    get_more(Connection, Database, Collection, Id, Opts);
get_more(Connection, #{<<"cursor">> := #{<<"id">> := _, <<"ns">> := _} = Cursor}, Opts) ->
    get_more(Connection, Cursor, Opts).

-spec get_more(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Id :: integer(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
get_more(Connection, Database, Collection, Id, Opts) ->
    run(Connection, Database, [{"getMore", #'bson.long'{value = Id}}, {"collection", Collection} | Opts]).

-spec insert(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Documents :: [bson:document()],
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
insert(Connection, Database, Collection, Documents, Opts) ->
    run(Connection, Database, [{insert, Collection}, {"documents", Documents} | Opts]).

-spec update(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Statements :: [bson:document()],
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
update(Connection, Database, Collection, Statements, Opts) ->
    run(Connection, Database, [{update, Collection}, {"updates", Statements} | Opts]).

%% === Administration Commands ===
%% https://docs.mongodb.com/manual/reference/command/#administration-commands

-spec compact(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
compact(Connection, Database, Collection, Opts) ->
    run(Connection, Database, [{compact, Collection} | Opts]).

-spec create_collection(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
create_collection(Connection, Database, Collection, Opts) ->
    run(Connection, Database, [{create, Collection} | Opts]).

-spec create_indexes(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Specs :: [bson:document()],
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
create_indexes(Connection, Database, Collection, Specs, Opts) ->
    run(Connection, Database, [{"createIndexes", Collection}, {"indexes", Specs} | Opts]).

-spec current_op(
    Connection :: mango:connection(),
    All :: boolean()
) -> mango_op_msg:response().
current_op(Connection, true) ->
    run(Connection, admin, [{"currentOp", 1}, {"$all", true}]);
current_op(Connection, false) ->
    run(Connection, admin, [{"currentOp", 1}, {"$ownOps", true}]).

-spec drop_collection(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
drop_collection(Connection, Database, Collection, Opts) ->
    run(Connection, Database, [{"drop", Collection} | Opts]).

-spec drop_database(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
drop_database(Connection, Database, Opts) ->
    run(Connection, Database, [{"dropDatabase", 1} | Opts]).

-spec drop_indexes(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Spec :: integer() | list(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
drop_indexes(Connection, Database, Collection, Spec, Opts) ->
    run(Connection, Database, [{"dropIndexes", Collection}, {"indexes", Spec} | Opts]).

-spec kill_cursor(
    Connection :: mango:connection(),
    Cursor :: mango:cursor(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
kill_cursor(Connection, #{<<"id">> := Id, <<"ns">> := Namespace}, Opts) ->
    [Database, Collection] = binary:split(Namespace, <<".">>),
    kill_cursors(Connection, Database, Collection, [Id], Opts);
kill_cursor(Connection, #{<<"cursor">> := #{<<"id">> := _, <<"ns">> := _} = Cursor}, Opts) ->
    kill_cursor(Connection, Cursor, Opts).

-spec kill_cursors(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Ids :: [integer()],
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
kill_cursors(Connection, Database, Collection, Ids, Opts) ->
    Cursors = lists:map(fun (Id) -> #'bson.long'{value = Id} end, Ids),
    run(Connection, Database, [{"killCursors", Collection}, {"cursors", Cursors} | Opts]).

-spec list_collections(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
list_collections(Connection, Database, Opts) ->
    run(Connection, Database, [{"listCollections", 1} | Opts]).

-spec list_databases(
    Connection :: mango:connection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
list_databases(Connection, Opts) ->
    run(Connection, admin, [{"listDatabases", 1} | Opts]).

-spec list_indexes(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
list_indexes(Connection, Database, Collection, Opts) ->
    run(Connection, Database, [{"listIndexes", Collection} | Opts]).

-spec re_index(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Collection :: mango:collection(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
re_index(Connection, Database, Collection, Opts) ->
    run(Connection, Database, [{"reIndex", Collection} | Opts]).

-spec rename_collection(
    Connection :: mango:connection(),
    Collection :: mango:namespace(),
    To :: mango:namespace(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
rename_collection(Connection, Collection, To, Opts) ->
    run(Connection, admin, [{"renameCollection", Collection}, {"to", To} | Opts]).

%% === Diagnostic Commands ===
%% https://docs.mongodb.com/manual/reference/command/#diagnostic-commands

-spec ping(
    Connection :: mango:connection()
) -> mango_op_msg:response().
ping(Connection) ->
    ping(Connection, admin).

-spec ping(
    Connection :: mango:connection(),
    Database :: mango:database()
) -> mango_op_msg:response().
ping(Connection, Database) ->
    case run(Connection, Database, [{ping, 1}]) of
        {ok, _} -> pong;
        {error, _} -> pang
    end.

-spec top(
    Connection :: mango:connection()
) -> mango_op_msg:response().
top(Connection) ->
    run(Connection, admin, [{top, 1}]).

-spec explain(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Command :: bson:document(),
    Opts :: proplists:proplist()
) -> mango_op_msg:response().
explain(Connection, Database, Command, Opts) ->
    run(Connection, Database, [{explain, Command} | Opts]).

%% === db.runCommand ===
%% run(Connection, Database, Command)
%% === db.adminCommand ===
%% run(Connection, admin, Command)

-spec run(
    Connection :: mango:connection(),
    Database :: mango:database(),
    Command :: t()
) -> mango_op_msg:response().
run(Connection, Database, [{_, _} = Command | Opts]) ->
    Request = mango_op_msg:request([Command, {"$db", Database} | Opts]),
    case mango_connection:request(Connection, Request) of
        {ok, Response} -> mango_op_msg:response(Response);
        {error, Reason} -> {error, Reason}
    end.
