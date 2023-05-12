%% @hidden
-module(mango_request_id).

-export([get/0]).

get() ->
    ets:update_counter(mango, request_id, {2, 1, 16#7fffffff, 0}, {request_id, 0}).
