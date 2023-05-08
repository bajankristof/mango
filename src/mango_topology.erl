-module(mango_topology).

-export([
    child_spec/2,
    start_link/1,
    stop/1,
    database/1,
    command/2,
    command/3
]).

-include("./_defaults.hrl").

-callback child_spec(Id :: supervisor:child_id(), Opts :: mango:start_opts()) -> supervisor:child_spec().
-callback start_link(Opts :: mango:start_opts()) -> gen_server:start_ret().
-callback stop(Connection :: mango:connection()) -> ok.
-callback database(Connection :: mango:connection()) -> mango:database().
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
    Module = module_from_start_opts(Opts),
    Module:child_spec(Id, start_opts(Opts)).

-spec start_link(Opts :: mango:start_opts()) -> gen_server:start_ret().
start_link(Opts) ->
    Module = module_from_start_opts(Opts),
    Module:start_link(start_opts(Opts)).

-spec stop(Connection :: mango:connection()) -> ok.
stop(Connection) ->
    Module = module_from_server_ref(Connection),
    Module:stop(Connection).

-spec database(Connection :: mango:connection()) -> mango:database().
database(Connection) ->
    Module = module_from_server_ref(Connection),
    Module:database(Connection).

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
    Module = module_from_server_ref(Connection),
    Module:command(Connection, Command, Timeout).

%% === Internal Functions ===

start_opts(#{hosts := [Host]} = Opts) ->
    Opts#{host => Host};
start_opts(#{} = Opts) ->
    Opts.

module_from_start_opts(#{hosts := [_]}) ->
    mango_standalone;
module_from_start_opts(#{hosts := _}) ->
    mango_replica_set;
module_from_start_opts(#{}) ->
    mango_standalone.

module_from_server_ref(Connection) when erlang:is_pid(Connection) ->
    module_from_pid(Connection);
module_from_server_ref(Name) when erlang:is_atom(Name) ->
    module_from_pid(erlang:whereis(Name));
module_from_server_ref({via, Module, Name}) when erlang:is_atom(Module) ->
    module_from_pid(Module:whereis_name(Name)).

module_from_pid(Connection) when erlang:is_pid(Connection) ->
    true = erlang:is_process_alive(Connection),
    {dictionary, Info} = erlang:process_info(Connection, dictionary),
    {'$mango_topology', Module} = lists:keyfind('$mango_topology', 1, Info),
    Module.
