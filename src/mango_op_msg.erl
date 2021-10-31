-module(mango_op_msg).

-export([encode/1, decode/1]).

-spec encode(Command :: map() | list()) -> mango_message:t().
encode(Command) when erlang:is_map(Command) orelse erlang:is_list(Command) ->
    Body = bson:construct([{uint32, 0}, {byte, 0}, {document, Command}]),
    mango_message:new(Body).

-spec decode(Message :: mango_message:t()) -> map().
decode(Message) when erlang:is_binary(Message) ->
    2013 = mango_message:op_code(Message),
    Body = mango_message:body(Message),
    {[_, _, Document], <<>>} = bson:destruct([uint32, byte, document], Body),
    Document.
