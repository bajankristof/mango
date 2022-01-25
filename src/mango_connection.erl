-module(mango_connection).

-behaviour(gen_server).

-export([
    database/1,
    command/2,
    command/3
]).
-export([
    start/1,
    start_link/1,
    child_spec/2,
    stop/1
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2
]).

-include("mango.hrl").
-include("./constants.hrl").

-record(state, {host, port, database, socket, queue = #{}, buffer = <<>>}).
-record(init_arg, {opts}).

-spec database(Connection :: mango:connection()) -> term().
database(Connection) ->
    do_call(Connection, database, ?DEFAULT_TIMEOUT).

%% @equiv command(Connection, Command, ?DEFAULT_TIMEOUT)
command(Connection, Command) ->
    command(Connection, Command, ?DEFAULT_TIMEOUT).

-spec command(
    Connection :: mango:connection(),
    Command :: mango_command:t(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
command(Connection, #'mango.command'{} = Command, Timeout) ->
    Message = mango_op_msg:encode(Command),
    case do_call(Connection, {call, Message}, Timeout) of
        {ok, Payload} ->
            Reply = mango_op_msg:decode(Payload),
            Status = maps:get(<<"ok">>, Reply),
            case Status of 1.0 -> {ok, Reply}; 0.0 -> {error, Reply} end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec start(Opts :: list() | map()) -> {ok, pid()} | {error, term()}.
start(Opts) ->
    do_start(start, Opts).

-spec start_link(Opts :: {worker, {init_arg, map()}} | list() | map()) ->
    {ok, pid()} | {error, term()}.
start_link({worker, #init_arg{opts = Opts}}) ->
    gen_server:start_link(?MODULE, Opts, []);
start_link(Opts) ->
    do_start(start_link, Opts).

-spec child_spec(Id :: term(), Opts :: list() | map()) -> map().
child_spec(Id, Opts) ->
    #{id => Id, start => {?MODULE, start_link, [Opts]}}.

-spec stop(Connection :: mango:connection()) -> ok.
stop(Connection) ->
    poolboy:stop(Connection).

init(Opts) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{
        host = maps:get(host, Opts, "127.0.0.1"),
        port = maps:get(port, Opts, 27017),
        database = maps:get(database, Opts)
    }, {continue, connect}}.

handle_call(database, _, #state{database = Database} = State) ->
    {reply, Database, State};
handle_call({call, _}, _, #state{socket = undefined} = State) ->
    {reply, {error, nosock}, State};
handle_call({call, <<_:16/binary, _/binary>> = Message}, Client, #state{socket = Socket, queue = Queue} = State) ->
    Id = mango_message:request_id(Message),
    ok = gen_tcp:send(Socket, Message),
    {noreply, State#state{queue = Queue#{Id => Client}}};
handle_call(_, _, State) ->
    {reply, {error, badarg}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_continue(connect, #state{socket = undefined, host = Host, port = Port} = State) ->
    {noreply, State, {continue, {socket, gen_tcp:connect(Host, Port, [binary])}}};
handle_continue({socket, {error, Reason}}, #state{host = Host, port = Port} = State) ->
    logger:error("mango: Connection to ~ts:~p failed due to ~p", [Host, Port, Reason]),
    maybe_reconnect(State);
handle_continue({socket, {ok, Socket}}, #state{socket = undefined} = State) ->
    {noreply, State#state{socket = Socket}}.

handle_info(connect, #state{socket = undefined} = State) ->
    {noreply, State, {continue, connect}};
handle_info({tcp, Socket, Payload}, #state{socket = Socket, buffer = Buffer} = State) ->
    loop_reply(<<Buffer/binary, Payload/binary>>, State#state{buffer = <<>>});
handle_info({tcp_closed, Socket}, #state{socket = Socket} = State) ->
    maybe_reconnect(State).

terminate(_, #state{socket = undefined}) -> ok;
terminate(_, #state{socket = Socket}) ->
    gen_tcp:close(Socket).

%% private functions

maybe_reconnect(#state{} = State) ->
    erlang:send_after(5000, self(), connect),
    {noreply, State#state{socket = undefined}}.

loop_reply(<<>>, #state{} = State) ->
    {noreply, State};
loop_reply(Payload, #state{queue = Queue} = State) ->
    case mango_message:read(Payload) of
        {fin, Reply, Remainder} ->
            Id = mango_message:response_to(Reply),
            Client = maps:get(Id, Queue, undefined),
            ok = gen_server:reply(Client, {ok, Reply}),
            loop_reply(Remainder, State#state{queue = maps:without([Id], Queue)});
        nofin -> {noreply, State#state{buffer = Payload}}
    end.

do_call(Connection, Request, Timeout) ->
    RequestId = poolboy:transaction(Connection, fun (Worker) ->
        gen_server:send_request(Worker, Request)
    end),
    case gen_server:wait_response(RequestId, Timeout) of
        {reply, Reply} -> Reply;
        {error, {Reason, _}} -> {error, Reason};
        timeout -> {error, timeout}
    end.

do_start(Fun, Opts) when erlang:is_list(Opts) ->
    do_start(Fun, maps:from_list(Opts));
do_start(Fun, Opts) ->
    poolboy:Fun([
        {worker_module, ?MODULE},
        {size, maps:get(pool_size, Opts, 10)},
        {max_overflow, 0},
        {strategy, fifo}
        | maps:to_list(maps:with([name], Opts))
    ], {worker, #init_arg{opts = Opts}}).
