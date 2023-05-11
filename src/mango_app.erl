%% @hidden
-module(mango_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_, _) ->
    ets:new(mango, [set, public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ChildSpecs = lists:map(fun ({Id, Opts}) ->
        mango:child_spec(Id, Opts)
    end, application:get_env(mango, connections, [])),
    mango_sup:start_link(ChildSpecs).

stop(_) -> ok.
