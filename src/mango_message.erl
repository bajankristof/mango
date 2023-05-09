%% @hidden
-module(mango_message).

-compile({no_auto_import, [length/1]}).

-export([new/2, new/3, new/4]).
-export([
    length/1,
    request_id/1,
    response_to/1,
    op_code/1,
    body/1,
    take/1
]).

-type t() :: binary().
-type header() :: binary().
-type body() :: binary().
-export_type([t/0, header/0, body/0]).

-spec new(OpCode :: non_neg_integer(), Body :: binary()) -> t().
new(OpCode, Body) ->
    new(OpCode, mango_request_id:get(), Body).

-spec new(
    OpCode :: non_neg_integer(),
    RequestId :: non_neg_integer(),
    Body :: binary()
) -> t().
new(OpCode, RequestId, Body) ->
    new(OpCode, RequestId, 0, Body).

-spec new(
    OpCode :: non_neg_integer(),
    RequestId :: non_neg_integer(),
    ResponseTo :: non_neg_integer(),
    Body :: binary()
) -> t().
new(OpCode, RequestId, ResponseTo, Body) when erlang:is_binary(Body) ->
    Length = erlang:byte_size(Body) + 16,
    Header = bson:construct([{int32, Length}, {int32, RequestId}, {int32, ResponseTo}, {int32, OpCode}]),
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

-spec take(Buffer :: binary()) -> {t(), binary()} | error.
take(Buffer) ->
    Length = length(Buffer),
    case Buffer of
        <<Message:Length/binary, Rest/binary>> ->
            {Message, Rest};
        _ -> error
    end.
