%% @hidden
-module(mango_sup).

-behaviour(supervisor).

-export([start_link/1, start_link/2]).
-export([init/1]).

%% === Server Functions ===

-spec start_link(ChildSpecs :: [supervisor:child_spec()]) -> gen_server:start_ret().
start_link(ChildSpecs) ->
    start_link(one_for_one, ChildSpecs).

-spec start_link(
    Strategy :: supervisor:strategy(),
    ChildSpecs :: [supervisor:child_spec()]
) -> gen_server:start_ret().
start_link(Strategy, ChildSpecs) ->
    supervisor:start_link(?MODULE, {Strategy, ChildSpecs}).

%% === Supervisor Callbacks ===

init({Strategy, ChildSpecs}) ->
    {ok, {{Strategy, 10, 60}, ChildSpecs}}.
