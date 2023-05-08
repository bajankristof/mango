%% @hidden
-module(mango_connection).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([
    child_spec/2,
    start_link/1,
    stop/1,
    database/1,
    command/2,
    command/3
]).
-export([
    socket/1
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-include("mango.hrl").
-include("./_defaults.hrl").

-record(state, {
    ref,
    host,
    port,
    database,
    socket
}).

-define(state(Socket, Opts), #state{
    ref = erlang:make_ref(),
    host = maps:get(host, Opts, ?DEFAULT_HOST),
    port = maps:get(port, Opts, ?DEFAULT_PORT),
    database = maps:get(database, Opts),
    socket = Socket
}).

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

-spec database(Connection :: mango:connection()) -> mango:database().
database(Connection) ->
    gen_server:call(Connection, database, ?DEFAULT_TIMEOUT).

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
command(Connection, #'mango.command'{} = Command, Timeout) ->
    Socket = socket(Connection),
    mango_socket:command(Socket, Command, Timeout).

%% === Low-level Functions ===

-spec socket(Connection :: mango:connection()) -> mango:socket().
socket(Connection) ->
    gen_server:call(Connection, socket, ?DEFAULT_TIMEOUT).

%% === Gen Server Callbacks ===

init(Opts) ->
    erlang:process_flag(trap_exit, true),
    case mango_socket:connect(Opts) of
        {ok, Socket} -> {ok, ?state(Socket, Opts)};
        {error, Reason} -> {stop, Reason}
    end.

handle_call(database, _, #state{database = Database} = State) ->
    {reply, Database, State};
handle_call(socket, _, #state{socket = Socket} = State) ->
    {reply, Socket, State};
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, #state{socket = Socket}) ->
    mango_socket:close(Socket).
