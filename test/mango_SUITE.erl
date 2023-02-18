-module(mango_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% SETUP

all() -> [
    {group, mango_connection},
    {group, mango_command},
    {group, mango}
].

groups() -> [
    {mango_connection, [parallel], [
        can_connect
    ]},
    {mango_command, [parallel], [
        can_ping_connection,
        can_list_databases,
        can_run_commands
    ]},
    {mango, [sequence], [
        can_insert_documents,
        can_find_documents,
        can_delete_documents
    ]}
].

init_per_suite(Config) ->
    application:ensure_all_started(mango),
    Config.

end_per_suite(_) ->
    application:stop(mango).

init_per_group(mango_connection, Config) ->
    [{group, mango_connection} | Config];
init_per_group(Group, Config) ->
    {ok, Connection} = mango:start(#{database => Group}),
    [{group, Group}, {connection, Connection} | Config].

end_per_group(mango_connection, _) -> ok;
end_per_group(_, Config) ->
    Connection = ?config(connection, Config),
    mango:stop(Connection).

init_per_testcase(Case, Config) ->
    [{'case', Case} | Config].

end_per_testcase(_, _) -> ok.

%% TESTS

%% === mango_connection === %%

can_connect(Config) ->
    Case = ?config('case', Config),
    {ok, Connection} = mango:start_link(#{database => Case}),
    ok = mango:stop(Connection).

%% === mango_command === %%

can_ping_connection(Config) ->
    Connection = ?config(connection, Config),
    Command = mango_command:ping(),
    ?assertMatch({ok, #{}}, mango:run_command(Connection, Command)).

can_list_databases(Config) ->
    Connection = ?config(connection, Config),
    Command = mango_command:list_databases([]),
    ?assertMatch({ok, #{<<"databases">> := [_|_]}}, mango:run_command(Connection, Command)).

can_run_commands(Config) ->
    Connection = ?config(connection, Config),
    Command = mango_command:new({"usersInfo", 1}, admin),
    ?assertMatch({ok, #{<<"users">> := _}}, mango:run_command(Connection, Command)).

%% === mango === %%

can_insert_documents(Config) ->
    Connection = ?config(connection, Config),
    ?assertMatch({ok, #{<<"n">> := 1}}, mango:insert(Connection, ?MODULE, #{})).

can_find_documents(Config) ->
    Connection = ?config(connection, Config),
    {ok, Cursor} = mango:find(Connection, ?MODULE, #{}),
    ?assertMatch({ok, [_|_]}, mango_cursor:exhaust(Cursor)).

can_delete_documents(Config) ->
    Connection = ?config(connection, Config),
    ?assertMatch({ok, #{<<"n">> := 1}}, mango:delete(Connection, ?MODULE, #{})).
