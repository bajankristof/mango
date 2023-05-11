-module(mango).

-import(mango_command, [opts/1]).

-export([child_spec/2, start_link/1, stop/1, start_opts/1]).
-export([aggregate/3, aggregate/4, aggregate/5]).
-export([count/2, count/3, count/4, count/5]).
-export([delete/3, delete/4, delete/5, delete/6]).
-export([distinct/3, distinct/4, distinct/5, distinct/6]).
-export([find/2, find/3, find/4, find/5]).
-export([find_and_remove/3, find_and_remove/4, find_and_remove/5]).
-export([find_and_update/4, find_and_update/5, find_and_update/6]).
-export([find_one/2, find_one/3, find_one/4, find_one/5]).
-export([insert/3, insert/4, insert/5]).
-export([update/4, update/6, update/7, update/8]).
-export([run_command/2, run_command/3]).
-export([where/1, pd/1, pd/2]).

-include("mango.hrl").
-include("defaults.hrl").

-type start_opts() :: #{
    name := gen_server:server_name(),
    host := inet:hostname(),
    port := inet:port_number(),
    hosts := [inet:hostname() | {inet:hostname(), inet:port_number()}],
    database := binary(),
    pool_size := pos_integer(),
    min_backoff := pos_integer(),
    max_backoff := pos_integer(),
    max_attempts := infinity | pos_integer(),
    read_preference := primary | primary_preferred | secondary | secondary_preferred | nearest,
    retry_reads := boolean(),
    retry_writes := boolean()
}.
-type connection() :: gen_server:server_ref().
-type database() :: atom() | binary().
-type collection() :: atom() | binary().
-type namespace() :: binary().
-type cursor() :: #'mango.cursor'{}.
-type command() :: #'mango.command'{}.
-export_type([
    start_opts/0,
    connection/0,
    database/0,
    collection/0,
    namespace/0,
    cursor/0,
    command/0
]).

%% === Topology Functions ===

-spec child_spec(Id :: supervisor:child_id(), Opts :: start_opts()) -> supervisor:child_spec().
child_spec(Id, Opts) ->
    mango_topology:child_spec(Id, Opts).

-spec start_link(Opts :: start_opts()) -> supervisor:on_start().
start_link(Opts) ->
    mango_topology:start_link(Opts).

-spec stop(Connection :: connection()) -> ok.
stop(Connection) ->
    mango_topology:stop(Connection).

-spec start_opts(Opts :: start_opts()) -> start_opts().
start_opts(#{hosts := [_|_] = Hosts0, database := _} = Opts0) ->
    Hosts = lists:map(fun
        ({Host, Port}) -> {Host, Port};
        (Host) -> {Host, ?DEFAULT_PORT}
    end, Hosts0),
    Opts = maps:without([host, port], Opts0),
    maps:merge(?DEFAULT_OPTS, Opts#{hosts => Hosts});
start_opts(#{database := _} = Opts0) ->
    Defaults = maps:merge(?DEFAULT_OPTS, #{host => ?DEFAULT_HOST, port => ?DEFAULT_PORT}),
    Opts = maps:without([hosts], Opts0),
    maps:merge(Defaults, Opts).

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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, cursor()} | {error, term()}.
aggregate(Connection, Collection, Pipeline, Opts0, Timeout) ->
    Opts = [{<<"cursor">>, #{<<"batchSize">> => 0}} | opts(Opts0)],
    Command = mango_command:aggregate('$', Collection, Pipeline, Opts),
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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, integer()} | {error, term()}.
count(Connection, Collection, Selector, Opts0, Timeout) ->
    Opts = [{<<"query">>, Selector} | opts(Opts0)],
    Command = mango_command:count('$', Collection, Opts),
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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
delete(Connection, Collection, Selector, Limit, Opts, Timeout) ->
    Statement = maps:from_list([{<<"q">>, Selector}, {<<"limit">>, Limit} | opts(Opts)]),
    Command = mango_command:delete('$', Collection, [Statement], []),
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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, list()} | {error, term()}.
distinct(Connection, Collection, Key, Selector, Opts0, Timeout) ->
    Opts = [{<<"query">>, Selector} | opts(Opts0)],
    Command = mango_command:distinct('$', Collection, Key, Opts),
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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, cursor()} | {error, term()}.
find(Connection, Collection, Selector, Opts0, Timeout) ->
    Opts = [{<<"filter">>, Selector}, {<<"batchSize">>, 0}, {<<"singleBatch">>, false} | opts(Opts0)],
    Command = mango_command:find('$', Collection, Opts),
    Connection, run_command(Connection, Command, Timeout).

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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> bson:document() | undefined | {error, term()}.
find_and_remove(Connection, Collection, Selector, Opts0, Timeout) ->
    Opts = [{<<"query">>, Selector}, {<<"remove">>, true} | opts(Opts0)],
    Command = mango_command:find_and_modify('$', Collection, Opts),
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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> bson:document() | undefined | {error, term()}.
find_and_update(Connection, Collection, Selector, Update, Opts0, Timeout) ->
    Opts = [{<<"query">>, Selector}, {<<"update">>, Update} | opts(Opts0)],
    Command = mango_command:find_and_modify('$', Collection, Opts),
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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> bson:document() | undefined | {error, term()}.
find_one(Connection, Collection, Selector, Opts0, Timeout) ->
    Opts = [{<<"filter">>, Selector}, {<<"batchSize">>, 1}, {<<"singleBatch">>, true} | opts(Opts0)],
    Command = mango_command:find('$', Collection, Opts),
    case run_command(Connection, Command, Timeout) of
        {ok, #'mango.cursor'{first_batch = [Document]}} -> Document;
        {ok, #'mango.cursor'{first_batch = []}} -> undefined;
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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
insert(Connection, Collection, #{} = Statement, Opts, Timeout) ->
    insert(Connection, Collection, [Statement], Opts, Timeout);
insert(Connection, Collection, Statement, Opts, Timeout) ->
    Command = mango_command:insert('$', Collection, Statement, Opts),
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
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
update(Connection, Collection, Selector, Update, Upsert, Multi, Opts, Timeout) ->
    Statement = maps:from_list([
        {<<"q">>, Selector},
        {<<"u">>, Update},
        {<<"upsert">>, Upsert},
        {<<"multi">>, Multi}
        | opts(Opts)]),
    Command = mango_command:update('$', Collection, [Statement], []),
    run_command(Connection, Command, Timeout).

%% === Command Functions ===

% @equiv run_command(Connection, Command, ?DEFAULT_TIMEOUT)
run_command(Connection, Command) ->
    run_command(Connection, Command, ?DEFAULT_TIMEOUT).

-spec run_command(
    Connection :: connection(),
    Command :: command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
run_command(Connection, #'mango.command'{} = Command, Timeout) when not erlang:is_pid(Connection) ->
    Pid = mango:where(Connection),
    true = erlang:is_process_alive(Pid),
    run_command(Pid, Command, Timeout);
run_command(Connection, #'mango.command'{} = Command0, Timeout) ->
    Opts = mango_topology:opts(Connection),
    Command = mango_command:patch(Command0, Opts),
    case {execute(Connection, Command, Timeout), should_retry(Command, Opts)} of
        {{ok, Result}, _} -> {ok, Result};
        {{error, #{<<"ok">> := 0.0} = Reason}, _} -> {error, Reason};
        {{error, _}, true} -> execute(Connection, Command, Timeout);
        {{error, Reason}, _} -> {error, Reason}
    end.

%% === Helper Functions ===

-spec where(Server :: gen_server:server_ref()) -> pid() | undefined.
where({via, Module, Name}) ->
    Module:whereis_name(Name);
where({global, Name}) ->
    global:whereis_name(Name);
where({local, Name}) when erlang:is_atom(Name) ->
    erlang:whereis(Name);
where(Name) when erlang:is_atom(Name) ->
    erlang:whereis(Name);
where(Pid) when erlang:is_pid(Pid) ->
    Pid.

-spec pd(Server :: gen_server:server_ref()) -> list().
pd(Server) ->
    Pid = where(Server),
    true = erlang:is_process_alive(Pid),
    {dictionary, Info} = erlang:process_info(Pid, dictionary),
    Info.

-spec pd(Server :: gen_server:server_ref(), Key :: term()) -> tuple() | false.
pd(Server, Key) ->
    lists:keyfind(Key, 1, pd(Server)).

%% === Internal Functions ===

-spec execute(
    Connection :: pid(),
    Command :: command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {ok, cursor()} | {error, term()}.
execute(Connection, Command, Timeout) ->
    case mango_topology:select_server(Connection, Command, Timeout) of
        {ok, Server} ->
            case mango_topology:command(Server, Command, Timeout) of
                {ok, #{<<"cursor">> := _} = Cursor} ->
                    {ok, mango_cursor:new(Server, Cursor)};
                {ok, Document} ->
                    {ok, Document};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec should_retry(Command :: command(), Opts :: start_opts()) -> boolean().
should_retry(#'mango.command'{type = read}, #{retry_reads := false}) -> false;
should_retry(#'mango.command'{type = write}, #{retry_writes := false}) -> false;
should_retry(_, _) -> true.
