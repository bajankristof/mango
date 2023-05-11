-module(mango_topology).

-export([
    child_spec/2,
    start_link/1,
    stop/1,
    select_server/2,
    select_server/3,
    command/2,
    command/3,
    opts/1
]).

-include("./_defaults.hrl").

-callback child_spec(Id :: supervisor:child_id(), Opts :: mango:start_opts()) -> supervisor:child_spec().
-callback start_link(Opts :: mango:start_opts()) -> gen_server:start_ret().
-callback stop(Connection :: mango:connection()) -> ok.
-callback select_server(
    Connection :: mango:connection(),
    Command :: mango:command()
) -> {ok, mango:connection()} | {error, term()}.
-callback select_server(
    Connection :: mango:connection(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, mango:connection()} | {error, term()}.
-callback command(
    Connection :: mango:connection(),
    Command :: mango:command()
) -> {ok, bson:document()} | {error, term()}.
-callback command(
    Connection :: mango:connection(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.

%% === API Functions ===

-spec child_spec(Id :: supervisor:child_id(), Opts :: mango:start_opts()) -> supervisor:child_spec().
child_spec(Id, Opts) ->
    Module = module_from_opts(Opts),
    Module:child_spec(Id, Opts).

-spec start_link(Opts :: mango:start_opts()) -> gen_server:start_ret().
start_link(Opts) ->
    Module = module_from_opts(Opts),
    Module:start_link(Opts).

-spec stop(Connection :: mango:connection()) -> ok.
stop(Connection) ->
    Module = module_from_connection(Connection),
    Module:stop(Connection).

-spec select_server(
    Connection :: mango:connection(),
    Command :: mango:command()
) -> {ok, mango:connection()} | {error, term()}.
select_server(Connection, Command) ->
    select_server(Connection, Command, ?DEFAULT_TIMEOUT).

-spec select_server(
    Connection :: mango:connection(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, mango:connection()} | {error, term()}.
select_server(Connection, Command, Timeout) ->
    Module = module_from_connection(Connection),
    Module:select_server(Connection, Command, Timeout).

-spec command(
    Connection :: mango:connection(),
    Command :: mango:command()
) -> {ok, bson:document()} | {error, term()}.
command(Connection, Command) ->
    command(Connection, Command, ?DEFAULT_TIMEOUT).

-spec command(
    Connection :: mango:connection(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
command(Connection, Command, Timeout) ->
    Module = module_from_connection(Connection),
    Module:command(Connection, Command, Timeout).

-spec opts(Connection :: mango:connection()) -> mango:start_opts().
opts(Connection) ->
    {'$mango_opts', #{} = Opts} = mango:pd(Connection, '$mango_opts'),
    Opts.

%% === Internal Functions ===

module_from_opts(#{hosts := _}) ->
    mango_replica_set;
module_from_opts(#{}) ->
    mango_standalone.

module_from_connection(Connection) ->
    {'$mango_topology', Module} = mango:pd(Connection, '$mango_topology'),
    Module.
