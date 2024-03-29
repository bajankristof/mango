%% @hidden
-module(mango_topology).

-behaviour(gen_server).

-export([
    child_spec/2,
    start_link/1,
    stop/1
]).
-export([
    info/1,
    select_server/2,
    select_server/3,
    run_command/2,
    run_command/3
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2
]).

-include("defaults.hrl").
-include("mango.hrl").

-record(state, {
    ref,
    opts,
    topology_info,
    supervisor,
    read_queue = [],
    write_queue = []
}).

%% === Lifecycle Functions ===

-spec child_spec(Id :: supervisor:child_id(), Opts :: mango:start_opts()) -> supervisor:child_spec().
child_spec(Id, Opts) ->
    #{id => Id, start => {?MODULE, start_link, [Opts]}, restart => transient}.

-spec start_link(Opts :: mango:start_opts()) -> gen_server:start_ret().
start_link(#{name := Name} = Opts) ->
    gen_server:start_link(Name, ?MODULE, Opts, []);
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(Topology :: gen_server:server_ref()) -> ok.
stop(Topology) ->
    gen_server:stop(Topology).

%% === API Functions ===

-spec info(Topology :: gen_server:server_ref()) -> tuple().
info(Topology) ->
    gen_server:call(Topology, info).

%% @equiv select_server(Topology, Operation, ?TIMEOUT).
select_server(Topology, Operation) ->
    select_server(Topology, Operation, ?TIMEOUT).

-spec select_server(
    Topology :: gen_server:server_ref(),
    Operation :: read | write,
    Timeout :: timeout()
) -> {ok, pid()} | {error, term()}.
select_server(Topology, Operation, Timeout) ->
    TopologyInfo = info(Topology),
    select_server(Topology, TopologyInfo, Operation, Timeout).

%% @equiv run_command(Topology, Command, ?TIMEOUT)
run_command(Topology, Command) ->
    run_command(Topology, Command, ?TIMEOUT).

-spec run_command(
    Topology :: gen_server:server_ref(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {ok, mango:cursor()} | {error, term()}.
run_command(Topology, #command{} = Command0, Timeout) ->
    TopologyInfo = info(Topology),
    Command = mango_topology_info:prepare(TopologyInfo, Command0),
    ShouldRetry = mango_topology_info:should_retry(TopologyInfo, Command0),
    case {run_command(Topology, TopologyInfo, Command, Timeout), ShouldRetry} of
        {{ok, Document}, _} ->
            {ok, Document};
        {{error, #{} = Reason}, _} ->
            {error, Reason};
        {{error, _}, true} ->
            run_command(Topology, Command0, Timeout);
        {{error, Reason}, _} ->
            {error, Reason}
    end.


%% === Gen Server Callbacks ===

init(#{url := URL} = Opts0) ->
    Opts = mango_url:parse(URL),
    init(maps:merge(maps:remove(url, Opts0), Opts));
init(#{database := _} = Opts) ->
    TopologyInfo = mango_topology_info:new(Opts),
    {ok, Supervisor} = mango_sup:start_link(simple_one_for_one, [mango_server_monitor:child_spec()]),
    {ok, #state{
        ref = erlang:make_ref(),
        opts = Opts,
        topology_info = TopologyInfo,
        supervisor = Supervisor
    }, {continue, connect}}.

handle_call(info, _, #state{topology_info = TopologyInfo} = State) ->
    {reply, TopologyInfo, State};
handle_call({select_server, read}, From, #state{read_queue = Queue} = State) ->
    {noreply, State#state{read_queue = [From | Queue]}};
handle_call({select_server, write}, From, #state{write_queue = Queue} = State) ->
    {noreply, State#state{write_queue = [From | Queue]}};
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({hello, Ref, ServerInfo}, #state{ref = Ref} = State0) ->
    State = update_server(State0, ServerInfo),
    #state{topology_info = TopologyInfo, read_queue = RQueue0, write_queue = WQueue0} = State,
    RQueue = consume_queue(RQueue0, mango_topology_info:select_server(TopologyInfo, read)),
    WQueue = consume_queue(WQueue0, mango_topology_info:select_server(TopologyInfo, write)),
    {noreply, State#state{read_queue = RQueue, write_queue = WQueue}};
handle_info(_, State) ->
    {noreply, State}.

handle_continue(connect, #state{opts = #{hosts := [_|_] = Hosts}} = State) ->
    {noreply, State, {continue, {connect, Hosts}}};
handle_continue(connect, #state{opts = Opts} = State) ->
    Host = maps:get(host, Opts, ?HOST),
    Port = maps:get(port, Opts, ?PORT),
    {noreply, State, {continue, {connect, [{Host, Port}]}}};
handle_continue({connect, [{Host, Port} | Hosts]}, State) ->
    {noreply, add_server(State, Host, Port), {continue, {connect, Hosts}}};
handle_continue({connect, [Host | Hosts]}, State) ->
    handle_continue({connect, [{Host, ?PORT} | Hosts]}, State);
handle_continue({connect, []}, State) ->
    {noreply, State}.

%% === Internal Functions ===

-spec add_server(
    State :: #state{},
    Host :: inet:hostname(),
    Port :: inet:port_number()
) -> #state{}.
add_server(#state{
    ref = Ref,
    opts = Opts,
    topology_info = TopologyInfo,
    supervisor = Supervisor
} = State, Host, Port) ->
    {ok, Connection} = mango_connection:start_link(Host, Port, Opts),
    {ok, _} = supervisor:start_child(Supervisor, [self(), Ref, Host, Port, Opts]),
    ServerInfo = mango_server_info:new(Host, Port, Connection),
    State#state{topology_info = mango_topology_info:add_server(TopologyInfo, ServerInfo)}.

-spec update_server(
    State :: #state{},
    ServerInfo :: tuple()
) -> #state{}.
update_server(#state{topology_info = TopologyInfo0} = State, ServerInfo) ->
    TopologyInfo = mango_topology_info:update_server(TopologyInfo0, ServerInfo),
    State#state{topology_info = TopologyInfo}.

-spec select_server(
    Topology :: gen_server:server_ref(),
    TopologyInfo :: tuple(),
    Operation :: read | write,
    Timeout :: timeout()
) -> {ok, pid()} | {error, term()}.
select_server(Topology, TopologyInfo, Operation, Timeout) ->
    case mango_topology_info:select_server(TopologyInfo, Operation) of
        {value, ServerInfo} -> {ok, mango_server_info:connection(ServerInfo)};
        false -> gen_server:call(Topology, {select_server, Operation}, Timeout)
    end.

-spec consume_queue(
    Queue :: [gen_server:from()],
    {value, ServerInfo :: tuple()} | false
) -> [gen_server:from()].
consume_queue(Queue, false) -> Queue;
consume_queue(Queue, {value, ServerInfo}) ->
    Connection = mango_server_info:connection(ServerInfo),
    Callback = fun (From) -> gen_server:reply(From, {ok, Connection}) end,
    lists:foreach(Callback, Queue),
    [].

-spec run_command(
    Topology :: gen_server:server_ref(),
    TopologyInfo :: tuple(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, bson:document() | mango:cursor()} | {error, term()}.
run_command(Topology, TopologyInfo, Command, Timeout) ->
    #command{type = Operation} = Command,
    case select_server(Topology, TopologyInfo, Operation, Timeout) of
        {ok, Connection} -> mango_connection:run_command(Connection, Command, Timeout);
        {error, Reason} -> {error, Reason}
    end.
