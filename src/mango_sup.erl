%% @hidden
-module(mango_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).
-define(FLAGS, #{
    strategy => one_for_one,
    intensity => 10,
    period => 60
}).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    Specs = lists:map(fun ({Id, Opts}) ->
        mango:child_spec(Id, Opts)
    end, application:get_env(mango, connections, [])),
    {ok, {?FLAGS, Specs}}.
