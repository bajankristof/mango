%% @hidden
-module(mango_standalone).

-behaviour(gen_server).
-behaviour(mango_topology).

-export([
    child_spec/2,
    start_link/1,
    stop/1,
    select_server/2,
    select_server/3,
    command/2,
    command/3
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-include("defaults.hrl").

%% === Topology Callbacks ===

-spec child_spec(Id :: supervisor:child_id(), Opts :: mango:start_opts()) -> supervisor:child_spec().
child_spec(Id, Opts) ->
    #{id => Id, start => {?MODULE, start_link, [Opts]}, restart => transient}.

-spec start_link(Opts :: mango:start_opts()) -> gen_server:start_ret().
start_link(#{name := Name} = Opts) ->
    gen_server:start_link(Name, ?MODULE, Opts, []);
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(Connection :: mango:connection()) -> ok.
stop(Connection) ->
    gen_server:stop(Connection).

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
select_server(Connection, _, _) ->
    {ok, Connection}.

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
    Message = mango_op_msg:encode(Command),
    Operation = poolboy:transaction(Connection, fun (Worker) ->
        mango_connection:dispatch(Worker, Message)
    end),
    mango_connection:await(Operation, Timeout).

%% === Gen Server Callbacks ===

init(#{database := _} = Opts0) ->
    Opts = mango:start_opts(Opts0),
    erlang:put('$mango_topology', ?MODULE),
    erlang:put('$mango_opts', Opts),
    poolboy:init({pool_args(Opts), maps:without([name], Opts)}).

handle_call(Request, From, State) ->
    poolboy:handle_call(Request, From, State).

handle_cast(Request, State) ->
    poolboy:handle_cast(Request, State).

handle_info(Info, State) ->
    poolboy:handle_info(Info, State).

code_change(Vsn, State, Extra) ->
    poolboy:code_change(Vsn, State, Extra).

terminate(Reason, State) ->
    poolboy:terminate(Reason, State).

%% === Internal Functions ===

pool_args(#{pool_size := Size} = Opts) ->
    [{worker_module, mango_connection},
        {size, Size},
        {max_overflow, 0},
        {strategy, fifo}
        | maps:to_list(maps:with([name], Opts))].
