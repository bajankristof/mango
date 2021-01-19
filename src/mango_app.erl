-module(mango_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_, _) ->
    ets:new(mango, [set, public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    mango_sup:start_link().

stop(_) -> ok.
