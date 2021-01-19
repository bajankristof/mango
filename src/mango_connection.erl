-module(mango_connection).

-behaviour(gen_server).

-export([
    request/2,
    request/3
]).
-export([
    start_link/1,
    start_link/2,
    child_spec/2,
    child_spec/3
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2
]).

-record(state, {host, port, socket, queue = #{}, buffer = <<>>}).

-spec request(
    Connection :: mango:connection(),
    Request :: mango_message:t()
) -> {ok, mango_message:t()} | {error, term()}.
request(Connection, Request) ->
    gen_server:call(Connection, Request).

-spec request(
    Connection :: mango:connection(),
    Request :: mango_message:t(),
    Timeout :: integer()
) -> {ok, mango_message:t()} | {error, term()}.
request(Connection, Request, Timeout) ->
    gen_server:call(Connection, Request, Timeout).

start_link(Opts) when erlang:is_list(Opts) ->
    start_link(maps:from_list(Opts));
start_link(Opts) when erlang:is_map(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

start_link(Via, Opts) when erlang:is_list(Opts) ->
    start_link(Via, maps:from_list(Opts));
start_link(Via, Opts) when erlang:is_map(Opts) ->
    gen_server:start_link(Via, ?MODULE, Opts, []).

child_spec(Id, Opts) ->
    #{id => Id, start => {?MODULE, start_link, [Opts]}}.

child_spec(Id, Via, Opts) ->
    #{id => Id, start => {?MODULE, start_link, [Via, Opts]}}.

init(Opts) ->
    {ok, #state{
        host = maps:get(host, Opts, "127.0.0.1"),
        port = maps:get(port, Opts, 27017)
    }, {continue, connect}}.

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
