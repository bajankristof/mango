%% @hidden
-module(mango_socket).

-export([
    connect/1,
    connect/2,
    connect/4,
    send/2,
    recv/2,
    recv/3,
    command/2,
    command/3,
    close/1
]).

-include("mango.hrl").
-include("./_defaults.hrl").

-spec connect(
    Opts :: mango:start_opts()
) -> {ok, mango:socket()} | {error, term()}.
connect(#{} = Opts) ->
    Backoff = mango_backoff:from_start_opts(Opts),
    connect(Opts, Backoff).

-spec connect(
    Opts :: mango:start_opts(),
    Backoff :: mango_backoff:t()
) -> {ok, mango:socket()} | {error, term()}.
connect(#{} = Opts, Backoff0) ->
    Host = maps:get(host, Opts, ?DEFAULT_HOST),
    Port = maps:get(port, Opts, ?DEFAULT_PORT),
    Module = maps:get(socket_module, Opts, ?DEFAULT_SOCKET_MODULE),
    SocketOpts = maps:get(socket_opts, Opts, []),
    MaxAttempts = maps:get(max_attempts, Opts, ?DEFAULT_MAX_ATTEMPTS),
    case mango_backoff:apply(Backoff0, {?MODULE, connect, [Module, Host, Port, SocketOpts]}) of
        {{ok, Socket}, _} ->
            {ok, Socket};
        {{error, Reason}, MaxAttempts, _} ->
            logger:notice("mango: Failed to connect to ~ts:~p due to ~p", [Host, Port, Reason]),
            logger:warning("mango: Exhausted connect attempts to ~ts:~p", [Host, Port]),
            {error, Reason};
        {{error, Reason}, _, Backoff} ->
            logger:notice("mango: Failed to connect to ~ts:~p due to ~p", [Host, Port, Reason]),
            mango_backoff:sleep(Backoff),
            connect(Opts, Backoff)
    end.

-spec connect(
    Module :: module(),
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Opts :: mango:socket_opts()
) -> {ok, mango:socket()} | {error, term()}.
connect(Module, Host, Port, Opts0) ->
    Opts = [binary, {active, false}, {packet, raw}, {nodelay, true} | Opts0],
    case Module:connect(Host, Port, Opts, ?DEFAULT_TIMEOUT) of
        {ok, Socket} -> {ok, #'mango.socket'{module = Module, socket = Socket}};
        {error, Reason} -> {error, Reason}
    end.

-spec send(Socket :: mango:socket(), Packet :: binary()) -> ok.
send(#'mango.socket'{module = Module, socket = Socket}, Packet) ->
    Module:send(Socket, Packet).

-spec recv(
    Socket :: mango:socket(),
    Length :: non_neg_integer()
) -> {ok, binary()} | {error, term()}.
recv(#'mango.socket'{} = Socket, Length) ->
    recv(Socket, Length, ?DEFAULT_TIMEOUT).

-spec recv(
    Socket :: mango:socket(),
    Length :: non_neg_integer(),
    Timeout :: timeout()
) -> {ok, binary()} | {error, term()}.
recv(#'mango.socket'{module = Module, socket = Socket}, Length, Timeout) ->
    Module:recv(Socket, Length, Timeout).

-spec command(
    Socket :: mango:socket(),
    Command :: mango:command()
) -> {ok, bson:document()} | {error, term()}.
command(#'mango.socket'{} = Socket, #'mango.command'{} = Command) ->
    command(Socket, Command, ?DEFAULT_TIMEOUT).

-spec command(
    Socket :: mango:socket(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
command(#'mango.socket'{} = Socket, #'mango.command'{} = Command, Timeout) ->
    case catch begin
        Message = mango_op_msg:encode(Command),
        ok = send(Socket, Message),
        {ok, Header} = recv(Socket, 16, Timeout),
        Length = mango_message:length(Header),
        {ok, Body} = recv(Socket, Length - 16, Timeout),
        RequestId = mango_message:request_id(Message),
        ResponseTo = mango_message:response_to(Header),
        {response_to, RequestId} = {response_to, ResponseTo},
        mango_op_msg:decode(<<Header/binary, Body/binary>>)
    end of
        #{<<"ok">> := 1.0} = Response -> {ok, Response};
        #{<<"ok">> := 0.0} = Response -> {error, Response};
        {'EXIT', {{badmatch, {error, Reason}}, _}} -> {error, Reason};
        {'EXIT', {Reason, _}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.

-spec close(Socket :: mango:socket()) -> ok.
close(#'mango.socket'{module = Module, socket = Socket}) ->
    Module:close(Socket).
