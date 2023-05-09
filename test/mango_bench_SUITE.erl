-module(mango_bench_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(COLLECTION, test).
-define(N_DOCUMENTS, 10_000).
-define(N_QUERY_BATCHES, 100).

-define(loopSync(Fun, Count), begin
    Seq = lists:seq(1, Count),
    lists:foreach(fun (X) -> Fun(X) end, Seq)
end).
-define(loopAsync(Fun, Count), begin
    Self = self(),
    Ref = erlang:make_ref(),
    Seq = lists:seq(1, Count),
    lists:foreach(fun (X) -> erlang:spawn_link(fun () -> Fun(X), Self ! Ref end) end, Seq),
    lists:foreach(fun (_) -> receive Ref -> ok end end, Seq)
end).

%% SETUP

all() -> [{group, benchmark}].

groups() -> [
    {benchmark, [sequence], [
        bench_sync,
        bench_async
    ]}
].

init_per_suite(Config) ->
    application:ensure_all_started(mango),
    {ok, Connection} = mango:start_link(#{database => benchmark}),
    true = erlang:unlink(Connection),
    [{connection, Connection} | Config].

end_per_suite(Config) ->
    Connection = ?config(connection, Config),
    mango:stop(Connection),
    application:stop(mango).

measure(Label, Fun) ->
    Start = erlang:system_time(millisecond),
    Fun(),
    End = erlang:system_time(millisecond),
    ct:print(info, ?MAX_IMPORTANCE, "~ts took ~pms", [Label, End - Start]).

bench_sync(Config) ->
    Connection = ?config(connection, Config),
    measure("INSERT SYNC", fun () ->
        ?loopSync(fun (N) ->
            {ok, _} = mango:insert(Connection, ?COLLECTION, #{n => N})
        end, ?N_DOCUMENTS)
    end),
    measure("FIND SYNC", fun () ->
        ?loopSync(fun (N) ->
            Match = N * (?N_DOCUMENTS / ?N_QUERY_BATCHES),
            {ok, Cursor} = mango:find(Connection, ?COLLECTION, #{n => #{<<"$lte">> => Match}}),
            {ok, _} = mango_cursor:exhaust(Cursor)
        end, ?N_QUERY_BATCHES)
    end),
    measure("DELETE SYNC", fun () ->
        {ok, _} = mango:delete(Connection, ?COLLECTION, #{})
    end).

bench_async(Config) ->
    Connection = ?config(connection, Config),
    measure("INSERT ASYNC", fun () ->
        ?loopAsync(fun (N) ->
            {ok, _} = mango:insert(Connection, ?COLLECTION, #{n => N})
        end, ?N_DOCUMENTS)
    end),
    measure("FIND ASYNC", fun () ->
        ?loopAsync(fun (N) ->
            Match = N * (?N_DOCUMENTS / ?N_QUERY_BATCHES),
            {ok, Cursor} = mango:find(Connection, ?COLLECTION, #{n => #{<<"$lte">> => Match}}),
            {ok, _} = mango_cursor:exhaust(Cursor)
        end, ?N_QUERY_BATCHES)
    end),
    measure("DELETE ASYNC", fun () ->
        {ok, _} = mango:delete(Connection, ?COLLECTION, #{})
    end).
