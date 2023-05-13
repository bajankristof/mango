-module(mango).

-import(mango_command, [opts/1]).

-export([child_spec/2, start_link/1, stop/1, info/1]).
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

-export_type([start_opts/0]).
-export_type([database/0, read_preference/0, read_preference_mode/0]).
-export_type([collection/0, command/0, cursor/0]).

-include("defaults.hrl").
-include("mango.hrl").

-type start_opts() :: #{
    name := gen_server:server_name(),
    host := inet:hostname(),
    port := inet:port_number(),
    hosts := [inet:hostname() | {inet:hostname(), inet:port_number()}],
    database := database(),
    max_attempts := infinity | pos_integer(),
    max_backoff := pos_integer(),
    min_backoff := pos_integer(),
    pool_size := pos_integer(),
    read_preference := read_preference(),
    retry_reads := boolean(),
    retry_writes := boolean()
}.

-type database() :: atom() | iolist().
-type read_preference() ::
    read_preference_mode() |
    #{mode := read_preference_mode(), max_staleness_seconds := pos_integer()}.
-type read_preference_mode() ::
    primary
    | primary_preferred
    | secondary
    | secondary_preferred
    | nearest.

-type collection() :: atom() | iolist().
-type command() :: #command{}.
-type cursor() :: #cursor{}.

%% === Topology Functions ===

-spec child_spec(Id :: supervisor:child_id(), Opts :: start_opts()) -> supervisor:child_spec().
child_spec(Id, Opts) ->
    mango_topology:child_spec(Id, Opts).

-spec start_link(Opts :: start_opts()) -> supervisor:on_start().
start_link(Opts) ->
    mango_topology:start_link(Opts).

-spec stop(Topology :: gen_server:server_ref()) -> ok.
stop(Topology) ->
    mango_topology:stop(Topology).

-spec info(Topology :: gen_server:server_ref()) -> tuple().
info(Topology) ->
    mango_topology:info(Topology).

%% === Aggregation Functions ===

%% @equiv aggregate(Topology, Collection, Pipeline, [])
aggregate(Topology, Collection, Pipeline) ->
    aggregate(Topology, Collection, Pipeline, []).

%% @equiv aggregate(Topology, Collection, Pipeline, Opts, ?TIMEOUT)
aggregate(Topology, Collection, Pipeline, Opts) ->
    aggregate(Topology, Collection, Pipeline, Opts, ?TIMEOUT).

-spec aggregate(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Pipeline :: bson:array(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, cursor()} | {error, term()}.
aggregate(Topology, Collection, Pipeline, Opts, Timeout) ->
    Command = mango_command:aggregate('$', Collection, Pipeline, Opts),
    run_command(Topology, Command, Timeout).

%% @equiv count(Topology, Collection, #{})
count(Topology, Collection) ->
    count(Topology, Collection, #{}).

%% @equiv count(Topology, Collection, Selector, [])
count(Topology, Collection, Selector) ->
    count(Topology, Collection, Selector, []).

%% @equiv count(Topology, Collection, Selector, Opts, ?TIMEOUT)
count(Topology, Collection, Selector, Opts) ->
    count(Topology, Collection, Selector, Opts, ?TIMEOUT).

-spec count(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, integer()} | {error, term()}.
count(Topology, Collection, Selector, Opts0, Timeout) ->
    Opts = [{<<"query">>, Selector} | opts(Opts0)],
    Command = mango_command:count('$', Collection, Opts),
    case run_command(Topology, Command, Timeout) of
        {ok, #{<<"n">> := Count}} -> {ok, Count};
        {error, Reason} -> {error, Reason}
    end.

%% @equiv delete(Topology, Collection, Selector, 0)
delete(Topology, Collection, Selector) ->
    delete(Topology, Collection, Selector, 0).

%% @equiv delete(Topology, Collection, Selector, Limit, [])
delete(Topology, Collection, Selector, Limit) ->
    delete(Topology, Collection, Selector, Limit, []).

%% @equiv delete(Topology, Collection, Selector, Limit, Opts, ?TIMEOUT)
delete(Topology, Collection, Selector, Limit, Opts) ->
    delete(Topology, Collection, Selector, Limit, Opts, ?TIMEOUT).

-spec delete(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Selector :: bson:document(),
    Limit :: integer(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
delete(Topology, Collection, Selector, Limit, Opts, Timeout) ->
    Statement = maps:from_list([{<<"q">>, Selector}, {<<"limit">>, Limit} | opts(Opts)]),
    Command = mango_command:delete('$', Collection, [Statement], []),
    run_command(Topology, Command, Timeout).

%% @equiv distinct(Topology, Collection, Key, #{})
distinct(Topology, Collection, Key) ->
    distinct(Topology, Collection, Key, #{}).

%% @equiv distinct(Topology, Collection, Key, Selector, [])
distinct(Topology, Collection, Key, Selector) ->
    distinct(Topology, Collection, Key, Selector, []).

%% @equiv distinct(Topology, Collection, Key, Selector, Opts, ?TIMEOUT)
distinct(Topology, Collection, Key, Selector, Opts) ->
    distinct(Topology, Collection, Key, Selector, Opts, ?TIMEOUT).

-spec distinct(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Key :: atom() | binary() | list(),
    Selector :: bson:document(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, list()} | {error, term()}.
distinct(Topology, Collection, Key, Selector, Opts0, Timeout) ->
    Opts = [{<<"query">>, Selector} | opts(Opts0)],
    Command = mango_command:distinct('$', Collection, Key, Opts),
    case run_command(Topology, Command, Timeout) of
        {ok, #{<<"values">> := Values}} -> {ok, Values};
        {error, Reason} -> {error, Reason}
    end.

%% === Query and Write Operation Functions ===

%% @equiv find(Topology, Collection, #{})
find(Topology, Collection) ->
    find(Topology, Collection, #{}).

%% @equiv find(Topology, Collection, Selector, [])
find(Topology, Collection, Selector) ->
    find(Topology, Collection, Selector, []).

%% @equiv find(Topology, Collection, Selector, Opts, ?TIMEOUT)
find(Topology, Collection, Selector, Opts) ->
    find(Topology, Collection, Selector, Opts, ?TIMEOUT).

-spec find(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, cursor()} | {error, term()}.
find(Topology, Collection, Selector, Opts0, Timeout) ->
    Opts = [{<<"filter">>, Selector} | opts(Opts0)],
    Command = mango_command:find('$', Collection, Opts),
    Topology, run_command(Topology, Command, Timeout).

%% @equiv find_and_remove(Topology, Collection, Selector, [])
find_and_remove(Topology, Collection, Selector) ->
    find_and_remove(Topology, Collection, Selector, []).

%% @equiv find_and_remove(Topology, Collection, Selector, Opts, ?TIMEOUT)
find_and_remove(Topology, Collection, Selector, Opts) ->
    find_and_remove(Topology, Collection, Selector, Opts, ?TIMEOUT).

-spec find_and_remove(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> bson:document() | undefined | {error, term()}.
find_and_remove(Topology, Collection, Selector, Opts0, Timeout) ->
    Opts = [{<<"query">>, Selector}, {<<"remove">>, true} | opts(Opts0)],
    Command = mango_command:find_and_modify('$', Collection, Opts),
    case run_command(Topology, Command, Timeout) of
        {error, Reason} -> {error, Reason};
        {ok, #{<<"value">> := null}} -> undefined;
        {ok, #{<<"value">> := Document}} -> Document;
        {ok, #{}} -> undefined
    end.

%% @equiv find_and_update(Topology, Collection, Selector, Update, [])
find_and_update(Topology, Collection, Selector, Update) ->
    find_and_update(Topology, Collection, Selector, Update, []).

%% @equiv find_and_update(Topology, Collection, Selector, Update, Opts, ?TIMEOUT)
find_and_update(Topology, Collection, Selector, Update, Opts) ->
    find_and_update(Topology, Collection, Selector, Update, Opts, ?TIMEOUT).

-spec find_and_update(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Selector :: bson:document(),
    Update :: bson:document(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> bson:document() | undefined | {error, term()}.
find_and_update(Topology, Collection, Selector, Update, Opts0, Timeout) ->
    Opts = [{<<"query">>, Selector}, {<<"update">>, Update} | opts(Opts0)],
    Command = mango_command:find_and_modify('$', Collection, Opts),
    case run_command(Topology, Command, Timeout) of
        {error, Reason} -> {error, Reason};
        {ok, #{<<"value">> := null}} -> undefined;
        {ok, #{<<"value">> := Document}} -> Document;
        {ok, #{}} -> undefined
    end.

%% @equiv find_one(Topology, Collection, #{})
find_one(Topology, Collection) ->
    find_one(Topology, Collection, #{}).

%% @equiv find_one(Topology, Collection, Selector, [])
find_one(Topology, Collection, Selector) ->
    find_one(Topology, Collection, Selector, []).

%% @equiv find_one(Topology, Collection, Selector, Opts, ?TIMEOUT)
find_one(Topology, Collection, Selector, Opts) ->
    find_one(Topology, Collection, Selector, Opts, ?TIMEOUT).

-spec find_one(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Selector :: bson:document(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> bson:document() | undefined | {error, term()}.
find_one(Topology, Collection, Selector, Opts0, Timeout) ->
    Opts = [{<<"filter">>, Selector}, {<<"batchSize">>, 1}, {<<"singleBatch">>, true} | opts(Opts0)],
    Command = mango_command:find('$', Collection, Opts),
    case run_command(Topology, Command, Timeout) of
        {ok, #cursor{batch = [Document]}} -> Document;
        {ok, #cursor{batch = []}} -> undefined;
        {error, Reason} -> {error, Reason}
    end.

%% @equiv insert(Topology, Collection, Statement, [])
insert(Topology, Collection, Statement) ->
    insert(Topology, Collection, Statement, []).

%% @equiv insert(Topology, Collection, Statement, Opts, ?TIMEOUT)
insert(Topology, Collection, Statement, Opts) ->
    insert(Topology, Collection, Statement, Opts, ?TIMEOUT).

-spec insert(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    What :: bson:document() | [bson:document()],
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
insert(Topology, Collection, #{} = Statement, Opts, Timeout) ->
    insert(Topology, Collection, [Statement], Opts, Timeout);
insert(Topology, Collection, Statement, Opts, Timeout) ->
    Command = mango_command:insert('$', Collection, Statement, Opts),
    run_command(Topology, Command, Timeout).

%% @equiv update(Topology, Collection, Selector, Update, false, false)
update(Topology, Collection, Selector, Update) ->
    update(Topology, Collection, Selector, Update, false, false).

%% @equiv update(Topology, Collection, Selector, Update, Upsert, Multi, [])
update(Topology, Collection, Selector, Update, Upsert, Multi) ->
    update(Topology, Collection, Selector, Update, Upsert, Multi, []).

%% @equiv update(Topology, Collection, Selector, Update, Upsert, Multi, Opts, ?TIMEOUT)
update(Topology, Collection, Selector, Update, Upsert, Multi, Opts) ->
    update(Topology, Collection, Selector, Update, Upsert, Multi, Opts, ?TIMEOUT).

-spec update(
    Topology :: gen_server:server_ref(),
    Collection :: collection(),
    Selector :: bson:document(),
    Update :: bson:document(),
    Upsert :: boolean(),
    Multi :: boolean(),
    Opts :: list() | map(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
update(Topology, Collection, Selector, Update, Upsert, Multi, Opts, Timeout) ->
    Statement = maps:from_list([
        {<<"q">>, Selector},
        {<<"u">>, Update},
        {<<"upsert">>, Upsert},
        {<<"multi">>, Multi}
        | opts(Opts)]),
    Command = mango_command:update('$', Collection, [Statement], []),
    run_command(Topology, Command, Timeout).

%% === Command Functions ===

% @equiv run_command(Topology, Command, ?TIMEOUT)
run_command(Topology, Command) ->
    run_command(Topology, Command, ?TIMEOUT).

-spec run_command(
    Topology :: gen_server:server_ref(),
    Command :: command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
run_command(Topology, #command{} = Command, Timeout) ->
    mango_topology:run_command(Topology, Command, Timeout).
