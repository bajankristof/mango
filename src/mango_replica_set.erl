%% @hidden
-module(mango_replica_set).

-behaviour(supervisor).
-behaviour(mango_topology).

-export([
    child_spec/2,
    start_link/1,
    stop/1,
    database/1,
    command/2,
    command/3
]).
-export([
    init/1
]).

-include("./_defaults.hrl").

-define(FLAGS, #{
    strategy => one_for_one,
    intensity => 10,
    period => 60
}).

%% === Topology Callbacks ===

-spec child_spec(Id :: supervisor:child_id(), Opts :: mango:start_opts()) -> supervisor:child_spec().
child_spec(Id, Opts) ->
    #{id => Id, start => {?MODULE, start_link, [Opts]}, restart => transient}.

-spec start_link(Opts :: mango:start_opts()) -> gen_server:start_ret().
start_link(#{name := Name} = Opts) ->
    supervisor:start_link(Name, ?MODULE, Opts);
start_link(Opts) ->
    supervisor:start_link(?MODULE, Opts).

-spec stop(Connection :: mango:connection()) -> ok.
stop(Connection) ->
    gen_server:stop(Connection).

-spec database(Connection :: mango:connection()) -> mango:database().
database(_Connection) ->
    error.

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
command(_Connection, _Command, _Timeout) ->
    error.

%% === Supervisor Callbacks ===

init(#{hosts := _Hosts} = Opts0) ->
    erlang:put('$mango_topology', ?MODULE),
    _Opts = maps:without([name, hosts], Opts0),
    {ok, {?FLAGS, []}}.
