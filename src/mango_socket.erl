%% @hidden
-module(mango_socket).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([
    child_spec/3,
    child_spec/4,
    start_link/1,
    start_link/2,
    start_link/3,
    stop/1
]).
-export([
    send/2,
    recv/1,
    recv/2
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2
]).

-include("defaults.hrl").
-include("mango.hrl").

-record(state, {
    host,
    port,
    max_attempts,
    max_backoff,
    min_backoff,
    socket,
    queue = #{},
    buffer = <<>>
}).

-define(connect(Host, Port), gen_tcp:connect(Host, Port, [binary, {nodelay, true}])).

%% === Lifecycle Functions ===

%% @equiv child_spec(Id, Host, Port, #{})
child_spec(Id, Host, Port) ->
    child_spec(Id, Host, Port, #{}).

-spec child_spec(
    Id :: supervisor:child_id(),
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Opts :: mango:start_opts()
) -> supervisor:child_spec().
child_spec(Id, Host, Port, Opts) ->
    #{id => Id, start => {?MODULE, start_link, [Host, Port, Opts]}, restart => transient}.

-spec start_link({
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Opts :: mango:start_opts()
}) -> gen_server:start_ret().
start_link({Host, Port, Opts}) ->
    start_link(Host, Port, Opts).

%% @equiv start_link(Host, Port, #{})
start_link(Host, Port) ->
    start_link(Host, Port, #{}).

-spec start_link(
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Opts :: mango:start_opts()
) -> gen_server:start_ret().
start_link(Host, Port, Opts) ->
    gen_server:start_link(?MODULE, {Host, Port, Opts}, []).

-spec stop(Socket :: pid()) -> ok.
stop(Socket) ->
    gen_server:stop(Socket).

%% === API Functions ===

-spec send(
    Socket :: pid(),
    Request :: binary()
) -> gen_server:request_id().
send(Socket, <<_:16/binary, _:1/binary, _/binary>> = Request) ->
    gen_server:send_request(Socket, {send, Request}).

%% @equiv recv(RequestId, ?TIMEOUT)
recv(RequestId) ->
    recv(RequestId, ?TIMEOUT).

-spec recv(
    RequestId :: gen_server:request_id(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
recv(RequestId, Timeout) ->
    case gen_server:receive_response(RequestId, Timeout) of
        {reply, Response} -> {ok, Response};
        {error, Reason} -> {error, Reason}
    end.

%% === Gen Server Callbacks ===

init({Host, Port, #{} = Opts}) ->
    {ok, #state{
        host = Host,
        port = Port,
        max_attempts = maps:get(max_attempts, Opts, ?MAX_ATTEMPTS),
        max_backoff = maps:get(max_backoff, Opts, ?MAX_BACKOFF),
        min_backoff = maps:get(min_backoff, Opts, ?MIN_BACKOFF)
    }, {continue, connect}}.

handle_call({send, Request}, From, #state{socket = Socket, queue = Queue} = State) ->
    Id = mango_message:request_id(Request),
    ok = gen_tcp:send(Socket, Request),
    {noreply, State#state{queue = Queue#{Id => From}}};
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Packet}, #state{socket = Socket, buffer = Buffer} = State) ->
    {noreply, State#state{buffer = <<Buffer/binary, Packet/binary>>}, {continue, packet}};
handle_info({tcp_closed, Socket}, #state{host = Host, port = Port, socket = Socket, queue = Queue} = State) ->
    logger:notice("mango: Connection to ~ts:~p closed, shutting down...", [Host, Port]),
    maps:foreach(fun (_, From) -> gen_server:reply(From, {error, tcp_closed}) end, Queue),
    {stop, tcp_closed, State};
handle_info(_, State) ->
    {noreply, State}.

handle_continue({message, Message}, #state{} = State) ->
    handle_message(Message, State);
handle_continue(packet, #state{} = State) ->
    handle_packet(State);
handle_continue(connect, #state{} = State) ->
    case connect(State) of
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

connect(#state{max_backoff = MaxBackoff, min_backoff = MinBackoff} = State) ->
    Backoff = mango_backoff:new(MaxBackoff, MinBackoff),
    connect(State, Backoff).

connect(#state{host = Host, port = Port, max_attempts = MaxAttempts} = State, Backoff) ->
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
            connect(State, mango_backoff:incr(Backoff))
    end.

handle_packet(#state{buffer = Buffer} = State) when erlang:byte_size(Buffer) < 16 ->
    {noreply, State};
handle_packet(#state{buffer = Buffer} = State) ->
    case mango_message:take(Buffer) of
        {Message, Rest} ->
            {noreply, State#state{buffer = Rest}, {continue, {message, Message}}};
        error -> {noreply, State}
    end.

handle_message(Message, #state{queue = Queue} = State) ->
    Id = mango_message:response_to(Message),
    From = maps:get(Id, Queue, undefined),
    catch gen_server:reply(From, Message),
    {noreply, State#state{queue = maps:remove(Id, Queue)}, {continue, packet}}.
