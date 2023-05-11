%% @hidden
-module(mango_sup).

-behaviour(supervisor).

-export([start_link/1, start_link/2]).
-export([init/1]).

-define(FLAGS, #{
    strategy => one_for_one,
    intensity => 10,
    period => 60
}).

%% === Server Functions ===

-spec start_link(ChildSpecs :: [supervisor:child_spec()]) -> gen_server:start_ret().
start_link(ChildSpecs) ->
    start_link(#{}, ChildSpecs).

-spec start_link(Flags :: supervisor:sup_flags(), ChildSpecs :: [supervisor:child_spec()]) -> gen_server:start_ret().
start_link(Flags, ChildSpecs) ->
    supervisor:start_link(?MODULE, {Flags, ChildSpecs}).

%% === Supervisor Callbacks ===

init({Flags, ChildSpecs}) ->
    {ok, {maps:merge(?FLAGS, Flags), ChildSpecs}}.
