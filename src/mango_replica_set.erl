%% @hidden
-module(mango_replica_set).

-behaviour(gen_server).

-export([
    start/1,
    start_link/1,
    child_spec/2,
    stop/1
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2
]).
