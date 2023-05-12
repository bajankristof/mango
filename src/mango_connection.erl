%% @hidden
-module(mango_connection).

-export([
    child_spec/3,
    start_link/2,
    start_link/3,
    stop/1
]).
-export([
    run_command/2,
    run_command/3
]).

-include("defaults.hrl").
-include("mango.hrl").

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

%% @equiv start_link(Host, Port, #{})
start_link(Host, Port) ->
    start_link(Host, Port, #{}).

-spec start_link(
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Opts :: mango:start_opts()
) -> gen_server:start_ret().
start_link(Host, Port, Opts) ->
    poolboy:start_link([
        {worker_module, mango_socket},
        {size, maps:get(pool_size, Opts, ?POOL_SIZE)},
        {max_overflow, 0},
        {strategy, fifo}
    ], {Host, Port, Opts}).

-spec stop(Connection :: pid()) -> ok.
stop(Connection) ->
    poolboy:stop(Connection).

%% === API Functions ===

%% @equiv run_command(Connection, Command, ?TIMEOUT)
run_command(Connection, Command) ->
    run_command(Connection, Command, ?TIMEOUT).

-spec run_command(
    Connection :: gen_server:server_ref(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
run_command(Connection, #command{} = Command, Timeout) ->
    Request = mango_op_msg:encode(Command),
    RequestId = poolboy:transaction(Connection, fun (Socket) ->
        mango_socket:send(Socket, Request)
    end),
    case mango_socket:recv(RequestId, Timeout) of
        {ok, Response} ->
            case mango_op_msg:decode(Response) of
                #{<<"ok">> := 1.0, <<"cursor">> := _} = Cursor ->
                    {ok, mango_cursor:new(Connection, Cursor)};
                #{<<"ok">> := 1.0} = Document ->
                    {ok, Document};
                #{<<"ok">> := 0.0} = Reason ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
