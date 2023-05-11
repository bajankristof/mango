%% @hidden
-module(mango_connection).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([
    child_spec/2,
    start_link/1,
    stop/1
]).
-export([
    dispatch/2,
    await/1,
    await/2
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2
]).

-include("mango.hrl").
-include("defaults.hrl").

-record(state, {
    opts,
    host,
    port,
    socket,
    queue = #{},
    buffer = <<>>
}).

-define(connect(Host, Port), gen_tcp:connect(Host, Port, [binary, {nodelay, true}])).

%% === Gen Server Functions ===

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

%% === Low-level Functions ===

-spec dispatch(
    Connection :: mango:connection(),
    Command :: mango:command() | mango_message:t()
) -> gen_server:request_id().
dispatch(Connection, #'mango.command'{} = Command) ->
    Message = mango_op_msg:encode(Command),
    dispatch(Connection, Message);
dispatch(Connection, Message) ->
    gen_server:send_request(Connection, {dispatch, Message}).

-spec await(
    Operation :: gen_server:request_id()
) -> {ok, bson:document()} | {error, term()}.
await(Operation) ->
    await(Operation, ?DEFAULT_TIMEOUT).

-spec await(
    Operation :: gen_server:request_id(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
await(Operation, Timeout) ->
    case catch begin
        {reply, Message} = gen_server:receive_response(Operation, Timeout),
        mango_op_msg:decode(Message)
    end of
        #{<<"ok">> := 1.0} = Document -> {ok, Document};
        #{<<"ok">> := 0.0} = Reason -> {error, Reason};
        {'EXIT', {{badmatch, {error, {Reason, _}}}, _}} -> {error, Reason};
        {'EXIT', {Reason, _}} -> {error, Reason}
    end.

%% === Gen Server Callbacks ===

init(#{} = Opts0) ->
    Opts = mango:start_opts(Opts0),
    {ok, #state{
        opts = Opts,
        host = maps:get(host, Opts),
        port = maps:get(port, Opts)
    }, {continue, connect}}.

handle_call({dispatch, Message}, From, #state{socket = Socket, queue = Queue} = State) ->
    Id = mango_message:request_id(Message),
    ok = gen_tcp:send(Socket, Message),
    {noreply, State#state{queue = Queue#{Id => From}}};
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Packet}, #state{socket = Socket, buffer = Buffer} = State) ->
    {noreply, State#state{buffer = <<Buffer/binary, Packet/binary>>}, {continue, state_change}};
handle_info({tcp_closed, Socket}, #state{host = Host, port = Port, socket = Socket, queue = Queue} = State) ->
    logger:notice("mango: Connection to ~ts:~p closed, shutting down...", [Host, Port]),
    maps:foreach(fun (_, From) -> gen_server:reply(From, {error, tcp_closed}) end, Queue),
    {stop, tcp_closed, State};
handle_info(_, State) ->
    {noreply, State}.

handle_continue({message, Message}, #state{} = State) ->
    handle_message(Message, State);
handle_continue(state_change, #state{} = State) ->
    handle_state_change(State);
handle_continue(connect, #state{opts = Opts} = State) ->
    case connect(Opts) of
        {ok, Socket} ->
            {noreply, State#state{socket = Socket}};
        {error, Reason} ->
            {stop, Reason, State}
    end.

terminate(_, #state{socket = undefined}) -> ok;
terminate(tcp_closed, _) -> ok;
terminate(_, #state{socket = Socket}) ->
    gen_tcp:close(Socket).

%% === Internal Functions ===

connect(Opts) ->
    connect(Opts, mango_backoff:from_start_opts(Opts)).

connect(#{host := Host, port := Port} = Opts, Backoff) ->
    MaxAttempts = maps:get(max_attempts, Opts),
    case {?connect(Host, Port), mango_backoff:n(Backoff, 1)} of
        {{ok, Socket}, _} ->
            {ok, [{sndbuf, Sndbuf}, {recbuf, Recbuf}, {buffer, Buffer}]} = inet:getopts(Socket, [sndbuf, recbuf, buffer]),
            ok = inet:setopts(Socket, [{buffer, lists:foldl(fun erlang:max/2, 16, [Sndbuf, Recbuf, Buffer])}]),
            {ok, Socket};
        {{error, Reason}, MaxAttempts} ->
            logger:notice("mango: Failed to connect to ~ts:~p due to ~p", [Host, Port, Reason]),
            logger:warning("mango: Exhausted connect attempts to ~ts:~p, shutting down...", [Host, Port]),
            {error, Reason};
        {{error, Reason}, _} ->
            logger:notice("mango: Failed to connect to ~ts:~p due to ~p", [Host, Port, Reason]),
            mango_backoff:sleep(Backoff),
            connect(Opts, mango_backoff:incr(Backoff))
    end.

handle_state_change(#state{buffer = Buffer} = State) when erlang:byte_size(Buffer) < 16 ->
    {noreply, State};
handle_state_change(#state{buffer = Buffer} = State) ->
    case mango_message:take(Buffer) of
        {Message, Rest} ->
            {noreply, State#state{buffer = Rest}, {continue, {message, Message}}};
        error -> {noreply, State}
    end.

handle_message(Message, #state{queue = Queue} = State) ->
    Id = mango_message:response_to(Message),
    From = maps:get(Id, Queue, undefined),
    catch gen_server:reply(From, Message),
    {noreply, State#state{queue = maps:remove(Id, Queue)}, {continue, state_change}}.
