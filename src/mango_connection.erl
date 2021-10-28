-module(mango_connection).

-behaviour(gen_server).

-export([
    database/1,
    request/2,
    request/3
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

-record(state, {host, port, database, socket, queue = #{}, buffer = <<>>}).
-record(init_arg, {opts}).

-spec database(Connection :: mango:connection()) -> term().
database(Connection) ->
    do_call(Connection, database, 10_000).

-spec request(
    Connection :: mango:connection(),
    Request :: mango_message:t()
) -> {ok, mango_message:t()} | {error, term()}.
request(Connection, Request) ->
    request(Connection, Request, 60_000).

-spec request(
    Connection :: mango:connection(),
    Request :: mango_message:t(),
    Timeout :: integer()
) -> {ok, mango_message:t()} | {error, term()}.
request(Connection, Request, Timeout) ->
    do_call(Connection, Request, Timeout).

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
handle_call(_, _, #state{socket = undefined} = State) ->
    {reply, {error, <<"ENOSOCK">>}, State};
handle_call(<<_:16/binary, _/binary>> = Request, Client, #state{socket = Socket, queue = Queue} = State) ->
    Id = mango_message:request_id(Request),
    ok = gen_tcp:send(Socket, Request),
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
        {ok, Response, Remainder} ->
            Id = mango_message:response_to(Response),
            Client = maps:get(Id, Queue, undefined),
            ok = gen_server:reply(Client, {ok, Response}),
            loop_reply(Remainder, State#state{queue = maps:without([Id], Queue)});
        _ -> {noreply, State#state{buffer = Payload}}
    end.

do_call(Connection, Request, Timeout) ->
    Ref = poolboy:transaction(Connection, fun (Worker) ->
        gen_server:send_request(Worker, Request)
    end),
    case gen_server:wait_response(Ref, Timeout) of
        {reply, Reply} -> Reply;
        {error, Reason} -> exit(Reason);
        timeout -> exit(timeout)
    end.

do_start(Fun, Opts) when erlang:is_list(Opts) ->
    do_start(Fun, maps:from_list(Opts));
do_start(Fun, Opts) ->
    poolboy:Fun([
        {worker_module, ?MODULE},
        {size, maps:get(pool_size, Opts, 10)},
        {max_overflow, maps:get(max_overflow, Opts, 0)}
        | maps:to_list(maps:with([name], Opts))
    ], {worker, #init_arg{opts = Opts}}).
