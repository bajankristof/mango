%% @hidden
-module(mango_op_msg).

-export([encode/1, decode/1]).

-include("mango.hrl").

-define(OP_CODE, 2013).

-spec encode(Command :: map() | list()) -> mango_message:t().
encode(#'mango.command'{command = Command, database = Database, opts = Opts}) ->
    encode([Command, {"$db", Database} | Opts]);
encode(Command) when erlang:is_map(Command) orelse erlang:is_list(Command) ->
    Body = bson:construct([{uint32, 0}, {byte, 0}, {document, Command}]),
    mango_message:new(?OP_CODE, Body).

-spec decode(Message :: mango_message:t()) -> map().
decode(Message) when erlang:is_binary(Message) ->
    ?OP_CODE = mango_message:op_code(Message),
    Body = mango_message:body(Message),
    {[_, _, Document], <<>>} = bson:destruct([uint32, byte, document], Body),
    Document.
