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
        can_connect_with_opts_map,
        can_connect_with_opts_list
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
    {ok, Connection} = mango_connection:start(#{database => Group}),
    [{group, Group}, {connection, Connection} | Config].

end_per_group(mango_connection, _) -> ok;
end_per_group(_, Config) ->
    Connection = ?config(connection, Config),
    mango_connection:stop(Connection).

init_per_testcase(Case, Config) ->
    [{'case', Case} | Config].

end_per_testcase(_, _) -> ok.

%% TESTS

%% === mango_connection === %%

can_connect_with_opts_map(Config) ->
    Case = ?config('case', Config),
    {ok, Connection} = mango_connection:start_link(#{database => Case}),
    ok = mango_connection:stop(Connection).

can_connect_with_opts_list(Config) ->
    Case = ?config('case', Config),
    {ok, Connection} = mango_connection:start_link([{database, Case}]),
    ok = mango_connection:stop(Connection).

%% === mango_command === %%

can_ping_connection(Config) ->
    Connection = ?config(connection, Config),
    ?assertMatch(pong, mango_command:ping(Connection)).

can_list_databases(Config) ->
    Connection = ?config(connection, Config),
    ?assertMatch({ok, #{<<"databases">> := [_|_]}}, mango_command:list_databases(Connection, [])).

can_run_commands(Config) ->
    Connection = ?config(connection, Config),
    ?assertMatch({ok, #{<<"users">> := _}}, mango_command:run(Connection, admin, [{"usersInfo", 1}])).

%% === mango === %%

can_insert_documents(Config) ->
    Connection = ?config(connection, Config),
    ?assertMatch({ok, #{<<"n">> := 1}}, mango:insert(Connection, ?MODULE, #{})).

can_find_documents(Config) ->
    Connection = ?config(connection, Config),
    {ok, Cursor} = mango:find(Connection, ?MODULE, #{}),
    ?assertMatch({ok, [_|_]}, mango_cursor:exhaust(Connection, Cursor)).

can_delete_documents(Config) ->
    Connection = ?config(connection, Config),
    ?assertMatch({ok, #{<<"n">> := 1}}, mango:delete(Connection, ?MODULE, #{})).
