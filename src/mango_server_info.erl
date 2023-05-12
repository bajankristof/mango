%% @hidden
-module(mango_server_info).

-export([
    new/3,
    from_hello/4,
    update/2
]).

-include("topology.hrl").

-type t() :: #server_info{}.

-define(helloStandalone(), #{<<"isWritablePrimary">> := true}).
-define(helloReplicaSet(IsPrimary, IsSecondary, LastWriteDate), #{
    <<"isWritablePrimary">> := IsPrimary,
    <<"secondary">> := IsSecondary,
    <<"lastWrite">> := #{<<"lastWriteDate">> := LastWriteDate}
}).

-spec new(
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Connection :: pid()
) -> t().
new(Host, Port, Connection) ->
    #server_info{host = Host, port = Port, connection = Connection}.

-spec from_hello(
    Host :: inet:hostname(),
    Port :: inet:port_number(),
    Rtt :: pos_integer(),
    Hello :: map()
) -> t().
from_hello(Host, Port, Rtt, ?helloReplicaSet(IsPrimary, IsSecondary, LastWriteDate)) ->
    Role = if IsPrimary -> primary; IsSecondary -> secondary; true -> undefined end,
    LastWrite = bson:datetime_to_ms(LastWriteDate),
    #server_info{role = Role, host = Host, port = Port, last_write = LastWrite, rtt = Rtt};
from_hello(Host, Port, Rtt, ?helloStandalone()) ->
    LastWrite = erlang:system_time(millisecond),
    #server_info{role = primary, host = Host, port = Port, last_write = LastWrite, rtt = Rtt}.

-spec update(ServerInfoA :: t(), ServerInfoB :: t()) -> t().
update(#server_info{host = Host, port = Port} = ServerInfoA, #server_info{host = Host, port = Port} = ServerInfoB) ->
    #server_info{role = Role, last_write = LastWrite, rtt = Rtt} = ServerInfoB,
    ServerInfoA#server_info{role = Role, last_write = LastWrite, rtt = Rtt};
update(#server_info{} = ServerInfo, #server_info{}) ->
    ServerInfo.

