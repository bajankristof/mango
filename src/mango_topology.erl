%% @hidden
-module(mango_topology).

-behaviour(gen_server).

-export([
    child_spec/2,
    start_link/1,
    stop/1
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2
]).

-include("defaults.hrl").

-record(state, {
    ref,
    opts,
    topology_info,
    supervisor,
    read_queue,
    write_queue
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

%% === Gen Server Callbacks ===

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
    {reply, {ok, TopologyInfo}, State};
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({update, Ref, ServerInfo}, #state{ref = Ref} = State) ->
    {noreply, update_server(ServerInfo, State)};
handle_info(_, State) ->
    {noreply, State}.

handle_continue(connect, #state{opts = #{hosts := [_|_] = Hosts}} = State) ->
    {noreply, State, {continue, {connect, Hosts}}};
handle_continue(connect, #state{opts = Opts} = State) ->
    Host = maps:get(host, Opts, ?HOST),
    Port = maps:get(port, Opts, ?PORT),
    {noreply, State, {continue, {connect, [{Host, Port}]}}};
handle_continue({connect, [{Host, Port} | Hosts]}, State) ->
    {noreply, add_server(Host, Port, State), {continue, {connect, Hosts}}};
handle_continue({connect, [Host | Hosts]}, State) ->
    handle_continue({connect, [{Host, ?PORT} | Hosts]}, State);
handle_continue({connect, []}, State) ->
    {noreply, State}.

%% === Internal Functions ===

add_server(Host, Port, #state{
    ref = Ref,
    opts = Opts,
    topology_info = TopologyInfo,
    supervisor = Supervisor
} = State) ->
    {ok, Connection} = mango_connection:start_link(Host, Port, Opts),
    {ok, _} = supervisor:start_child(Supervisor, [self(), Ref, Host, Port, Opts]),
    ServerInfo = mango_server_info:new(Host, Port, Connection),
    State#state{topology_info = mango_topology_info:add_server(TopologyInfo, ServerInfo)}.

update_server(ServerInfo, #state{topology_info = TopologyInfo} = State) ->
    State#state{topology_info = mango_topology_info:update_server(TopologyInfo, ServerInfo)}.
