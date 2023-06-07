%% @hidden
-module(mango_server_monitor).

-behaviour(gen_server).

-export([
    child_spec/0,
    child_spec/6,
    start_link/5,
    stop/1
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2
]).

-record(state, {topology, ref, host, port, opts, socket}).

-define(HEARTBEAT_INTERVAL, 10_000).

%% === Server Functions ===

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{id => ?MODULE, start => {?MODULE, start_link, []}, restart => transient}.

-spec child_spec(
    Id :: supervisor:child_id(),
    Topology :: pid(),
    Ref :: reference(),
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Opts :: mango:start_opts()
) -> supervisor:child_spec().
child_spec(Id, Topology, Ref, Host, Port, Opts) when erlang:is_pid(Topology) ->
    #{id => Id, start => {?MODULE, start_link, [Topology, Ref, Host, Port, Opts]}, restart => transient}.

-spec start_link(
    Topology :: pid(),
    Ref :: reference(),
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Opts :: mango:start_opts()
) -> gen_server:start_ret().
start_link(Topology, Ref, Host, Port, Opts) when erlang:is_pid(Topology) ->
    gen_server:start_link(?MODULE, {Topology, Ref, Host, Port, Opts}, []).

-spec stop(Server :: gen_server:server_ref()) -> ok.
stop(Server) ->
    gen_server:stop(Server).

%% === Gen Server Callbacks ===

init({Topology, Ref, Host, Port, Opts}) when erlang:is_pid(Topology) ->
    {ok, #state{
        topology = Topology,
        ref = Ref,
        host = Host,
        port = Port,
        opts = Opts
    }, {continue, connect}}.

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({heartbeat, Ref}, #state{ref = Ref} = State) ->
    {hello, Latency, Hello} = hello(State),
    {noreply, State, {continue, {hello, Latency, Hello}}};
handle_info(_, State) ->
    {noreply, State}.

handle_continue(connect, #state{ref = Ref, host = Host, port = Port, opts = Opts} = State) ->
    {ok, Socket} = mango_socket:start_link(Host, Port, Opts),
    self() ! {heartbeat, Ref},
    {noreply, State#state{socket = Socket}};
handle_continue({hello, _, #{<<"ok">> := 0.0}}, #state{topology = Topology, ref = Ref, host = Host, port = Port} = State) ->
    ServerInfo = mango_server_info:new(Host, Port, undefined),
    Topology ! {hello, Ref, ServerInfo},
    erlang:send_after(?HEARTBEAT_INTERVAL, self(), {heartbeat, Ref}),
    {noreply, State};
handle_continue({hello, Latency, Hello}, #state{topology = Topology, ref = Ref, host = Host, port = Port} = State) ->
    ServerInfo = mango_server_info:from_hello(Host, Port, Latency, Hello),
    Topology ! {hello, Ref, ServerInfo},
    erlang:send_after(?HEARTBEAT_INTERVAL, self(), {heartbeat, Ref}),
    {noreply, State}.

terminate(_, _) ->
    ok.

hello(#state{socket = Socket}) ->
    Request = mango_op_msg:encode(mango_command:hello()),
    Start = erlang:system_time(millisecond),
    RequestId = mango_socket:send(Socket, Request),
    {ok, Response} = mango_socket:recv(RequestId, ?HEARTBEAT_INTERVAL),
    Latency = erlang:system_time(millisecond) - Start,
    {hello, Latency, mango_op_msg:decode(Response)}.
