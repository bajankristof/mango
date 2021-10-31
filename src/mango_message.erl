-module(mango_message).

-compile({no_auto_import, [length/1]}).

-export([new/1]).
-export([
    length/1,
    request_id/1,
    response_to/1,
    op_code/1,
    body/1
]).
-export([read/1]).

-type t() :: binary().
-type header() :: binary().
-type body() :: binary().
-export_type([t/0, header/0, body/0]).

-spec new(Body :: binary()) -> t().
new(Body) when erlang:is_binary(Body) ->
    Length = erlang:byte_size(Body) + 16,
    Header = bson:construct([
        %% MsgHeader : messageLength, requestId, responseTo, opCode (OP_MSG by default)
        {int32, Length}, {int32, mango_request_id:get()}, {int32, 0}, {int32, 2013}]),
    <<Header/binary, Body/binary>>.

-spec length(Message :: t()) -> integer().
length(<<Chunk:4/binary, _/binary>>) ->
    {[Length], <<>>} = bson:destruct([int32], Chunk), Length.

-spec request_id(Message :: t()) -> integer().
request_id(<<_:4/binary, Chunk:4/binary, _/binary>>) ->
    {[Id], <<>>} = bson:destruct([int32], Chunk), Id.

-spec response_to(Message :: t()) -> integer().
response_to(<<_:8/binary, Chunk:4/binary, _/binary>>) ->
    {[Id], <<>>} = bson:destruct([int32], Chunk), Id.

-spec op_code(Message :: t()) -> integer().
op_code(<<_:12/binary, Chunk:4/binary, _/binary>>) ->
    {[Code], <<>>} = bson:destruct([int32], Chunk), Code.

-spec body(Message :: t()) -> binary().
body(<<_:16/binary, Body/binary>>) -> Body.

-spec read(Payload :: binary()) -> {fin, t(), binary()} | nofin.
read(Payload) when erlang:is_binary(Payload) ->
    Assert = length(Payload),
    case Payload of
        <<Message:Assert/binary, Remainder/binary>> ->
            {fin, Message, Remainder};
        _ -> nofin
    end.
