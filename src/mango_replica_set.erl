%% @hidden
-module(mango_replica_set).

-behaviour(gen_server).
-behaviour(mango_topology).

-export([
    child_spec/2,
    start_link/1,
    stop/1,
    select_server/2,
    select_server/3,
    command/2,
    command/3
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2
]).

-include("mango.hrl").
-include("defaults.hrl").
-include("status.hrl").

-record(state, {
    opts,
    ref,
    hosts,
    monitors,
    servers = #{},
    queues = #{primary => [], secondary => [], '_' => []}
}).

-define(storage(), ets:new(?MODULE, [set, public, {keypos, 2}, {write_concurrency, true}, {read_concurrency, true}])).
-define(monitors(), mango_sup:start_link(#{strategy => simple_one_for_one}, [mango_replica_monitor:child_spec()])).
-define(ms(Role), [{#'mango.status'{node = '_', role = Role, ts = '_', ex = '$1', rtt = '_'}, [{'>=','$1',erlang:system_time(millisecond)}], ['$_']}]).

%% === Topology Callbacks ===

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

-spec select_server(
    Connection :: mango:connection(),
    Command :: mango:command()
) -> {ok, mango:connection()} | {error, term()}.
select_server(Connection, Command) ->
    select_server(Connection, Command, ?DEFAULT_TIMEOUT).

-spec select_server(
    Connection :: mango:connection(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, mango:connection()} | {error, term()}.
select_server(Connection, Command, Timeout) ->
    Info = mango:pd(Connection),
    {'$mango_storage', Storage} = lists:keyfind('$mango_storage', 1, Info),
    {'$mango_opts', Opts} = lists:keyfind('$mango_opts', 1, Info),
    Role = select_role(Command, Opts),
    case select_nodes(Storage, Role, Opts) of
        [_|_] = Nodes -> gen_server:call(Connection, {checkout, Nodes}, Timeout);
        [] -> gen_server:call(Connection, {select, Role}, Timeout)
    end.

-spec command(
    Connection :: mango:connection(),
    Command :: mango:command()
) -> {ok, bson:document()} | {error, term()}.
command(Connection, Command) ->
    command(Connection, Command, ?DEFAULT_TIMEOUT).

-spec command(
    Connection :: mango:connection(),
    Command :: mango:command(),
    Timeout :: timeout()
) -> {ok, bson:document()} | {error, term()}.
command(Connection, Command, Timeout) ->
    {error, {Connection, Command, Timeout}}.

%% === Gen Server Callbacks ===

init(#{hosts := [_|_], database := _} = Opts0) ->
    Opts = mango:start_opts(Opts0),
    Storage = ?storage(),
    erlang:put('$mango_topology', ?MODULE),
    erlang:put('$mango_storage', Storage),
    erlang:put('$mango_opts', Opts),
    {ok, Monitors} = ?monitors(),
    {ok, #state{
        ref = erlang:make_ref(),
        opts = Opts,
        hosts = maps:get(hosts, Opts),
        monitors = Monitors
    }, {continue, connect}}.

handle_call({select, Role}, From, #state{queues = Queues} = State)
        when Role =:= primary
        orelse Role =:= secondary
        orelse Role =:= '_' ->
    Queue = [From | maps:get(Role, Queues)],
    {noreply, State#state{queues = Queues#{Role => Queue}}};
handle_call({checkout, [Node]}, From, #state{} = State) ->
    handle_call({checkout, Node, []}, From, State);
handle_call({checkout, [Default | _] = Nodes}, From, #state{} = State) ->
    handle_call({checkout, Default, Nodes}, From, State);
handle_call({checkout, Node, []}, _, #state{servers = Servers} = State) ->
    {reply, {ok, maps:get(Node, Servers)}, State};
handle_call({checkout, Default, [Node | Nodes]}, From, #state{servers = Servers} = State) ->
    Server = maps:get(Node, Servers),
    case gen_server:call(Server, status) of
        {ready, _, _, _} -> {reply, {ok, Server}, State};
        _ -> handle_call({checkout, Default, Nodes}, From, State)
    end;
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({monitor, Ref, Status}, #state{ref = Ref, servers = Servers, queues = Queues} = State) ->
    #'mango.status'{node = Node, role = Role} = Status,
    Server = maps:get(Node, Servers),
    Callback = fun (From) -> gen_server:reply(From, {ok, Server}) end,
    lists:foreach(Callback, lists:reverse(maps:get(Role, Queues))),
    lists:foreach(Callback, lists:reverse(maps:get('_', Queues))),
    {noreply, State#state{queues = Queues#{Role => [], '_' => []}}};
handle_info(_, State) ->
    {noreply, State}.

handle_continue(connect, #state{} = State) ->
    Servers = connect(State),
    {noreply, State#state{servers = Servers}}.

%% === Internal Functions ===

connect(#state{opts = Opts0, ref = Ref, hosts = Hosts, monitors = Monitors}) ->
    lists:foldl(fun ({Host, Port}, Acc) ->
        Opts = maps:without([name, hosts], Opts0#{host => Host, port => Port}),
        Node = lists:flatten(io_lib:format("~ts:~p", [Host, Port])),
        {ok, _} = supervisor:start_child(Monitors, [{self(), Ref, Node, Opts}]),
        {ok, Server} = mango_standalone:start_link(Opts),
        Acc#{Node => Server}
    end, #{}, Hosts).

select_role(#'mango.command'{type = write}, _) -> primary;
select_role(_, #{read_preference := primary}) -> primary;
select_role(_, #{read_preference := secondary}) -> secondary;
select_role(_, _) -> '_'.

select_nodes(Storage, Role, Opts) ->
    Records = sort_status_records(ets:select(Storage, ?ms(Role)), Opts),
    lists:map(fun (#'mango.status'{node = Node}) -> Node end, Records).

sort_status_records(Records, #{read_preference := primary_preferred} = Opts) ->
    sort_status_records(Records, primary, Opts);
sort_status_records(Records, #{read_preference := secondary_preferred} = Opts) ->
    sort_status_records(Records, secondary, Opts);
sort_status_records(Records, Opts) ->
    sort_status_records(Records, '_', Opts).

sort_status_records(Records, Role, _) ->
    lists:sort(fun
        (#'mango.status'{role = R}, _) when R =:= Role -> true;
        (_, #'mango.status'{role = R}) when R =:= Role -> false;
        (#'mango.status'{rtt = A}, #'mango.status'{rtt = B}) when A =< B -> true;
        (_, _) -> false
    end, Records).
