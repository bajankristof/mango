-module(mango_replica_monitor).

-behaviour(gen_server).

-export([
    child_spec/0,
    child_spec/2,
    start_link/1,
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

-include("status.hrl").

-record(state, {rs, ref, node, opts, storage, connection}).

-type start_args() :: {Rs :: pid(), Node :: iolist(), Opts :: mango:start_opts()}.
-export_type([start_args/0]).

-define(HEARTBEAT_INTERVAL_MS, 10_000).

-define(role(Status), case Status of
    #{<<"isWritablePrimary">> := true} -> primary;
    #{<<"secondary">> := true} -> secondary;
    _ -> undefined
end).

%% === Server Functions ===

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{id => ?MODULE, start => {?MODULE, start_link, []}, restart => transient}.

-spec child_spec(Id :: supervisor:child_id(), Args :: start_args()) -> supervisor:child_spec().
child_spec(Id, {Rs, Ref, Node, Opts}) when erlang:is_pid(Rs) ->
    #{id => Id, start => {?MODULE, start_link, [{Rs, Ref, Node, Opts}]}, restart => transient}.

-spec start_link(Args :: start_args()) -> gen_server:start_ret().
start_link({Rs, Ref, Node, Opts}) when erlang:is_pid(Rs) ->
    gen_server:start_link(?MODULE, {Rs, Ref, Node, Opts}, []).

-spec stop(Monitor :: mango:connection()) -> ok.
stop(Monitor) ->
    gen_server:stop(Monitor).

%% === Gen Server Callbacks ===

init({Rs, Ref, Node, Opts}) when erlang:is_pid(Rs) ->
    {'$mango_storage', Storage} = mango:pd(Rs, '$mango_storage'),
    {ok, #state{
        rs = Rs,
        ref = Ref,
        node = Node,
        opts = Opts,
        storage = Storage
    }, {continue, connect}}.

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({fetch, Ref}, #state{ref = Ref} = State) ->
    {noreply, State, {continue, fetch}};
handle_info(_, State) ->
    {noreply, State}.

handle_continue(connect, #state{opts = Opts} = State) ->
    {ok, Connection} = mango_connection:start_link(Opts),
    {noreply, State#state{connection = Connection}, {continue, fetch}};
handle_continue(fetch, #state{rs = Rs, ref = Ref, storage = Storage} = State) ->
    Status = fetch(State),
    ets:insert(Storage, Status),
    erlang:send_after(0, Rs, {monitor, Ref, Status}),
    erlang:send_after(?HEARTBEAT_INTERVAL_MS, self(), {fetch, Ref}),
    {noreply, State}.

terminate(_, #state{node = Node, storage = Storage}) ->
    ets:delete(Storage, Node).

%% === Internal Functions ===

fetch(#state{node = Node, connection = Connection}) ->
    Start = erlang:system_time(millisecond),
    Operation = mango_connection:dispatch(Connection, mango_command:hello()),
    {ok, Status} = mango_connection:await(Operation),
    Now = erlang:system_time(millisecond),
    #'mango.status'{node = Node, role = ?role(Status), ts = Now, ex = Now + (2 * ?HEARTBEAT_INTERVAL_MS), rtt = Now - Start}.
