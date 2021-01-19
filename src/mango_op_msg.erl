-module(mango_op_msg).

-export([request/1, response/1]).

-type t() :: mango_message:t().
-type request() :: mango_message:t().
-type response() :: {ok, map()} | {error, term()}.
-export_type([t/0, request/0, response/0]).

-spec request(Command :: map() | list()) -> request().
request(Command) when erlang:is_map(Command) orelse erlang:is_list(Command) ->
    Body = bson:construct([{uint32, 0}, {byte, 0}, {document, Command}]),
    mango_message:new(Body).

-spec response(Message :: t()) -> response().
response(Message) when erlang:is_binary(Message) ->
    2013 = mango_message:op_code(Message),
    Body = mango_message:body(Message),
    case bson:destruct([uint32, byte, document], Body) of
        {[_, _, #{<<"ok">> := 1.0} = Content], <<>>} ->
            {ok, maps:without([<<"ok">>], Content)};
        {[_, _, #{<<"ok">> := 0.0} = Content], <<>>} ->
            {error, maps:without([<<"ok">>], Content)}
    end.
