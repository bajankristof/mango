%% @hidden
-module(mango_connection).

-export([
    child_spec/3,
    start_link/2,
    start_link/3,
    stop/1
]).

-include("defaults.hrl").

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
