%% @hidden
-module(mango_server_info).

-export([
    new/3,
    from_hello/4,
    role/1,
    host/1,
    port/1,
    connection/1,
    last_write/1,
    latency/1,
    update/2,
    compare/3,
    compare_role/3,
    compare_latency/2,
    is_unknown/1,
    is_stale/3
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
    Latency :: pos_integer(),
    Hello :: map()
) -> t().
from_hello(Host, Port, Latency, ?helloReplicaSet(IsPrimary, IsSecondary, LastWriteDate)) ->
    Role = if IsPrimary -> primary; IsSecondary -> secondary; true -> undefined end,
    LastWrite = bson:datetime_to_ms(LastWriteDate),
    #server_info{role = Role, host = Host, port = Port, last_write = LastWrite, latency = Latency};
from_hello(Host, Port, Latency, ?helloStandalone()) ->
    LastWrite = erlang:system_time(millisecond),
    #server_info{role = primary, host = Host, port = Port, last_write = LastWrite, latency = Latency}.

-spec role(ServerInfo :: t()) -> primary | secondary | undefined.
role(#server_info{role = Role}) ->
    Role.

-spec host(ServerInfo :: t()) -> primary | secondary | undefined.
host(#server_info{host = Host}) ->
    Host.

-spec port(ServerInfo :: t()) -> primary | secondary | undefined.
port(#server_info{port = Port}) ->
    Port.

-spec connection(ServerInfo :: t()) -> primary | secondary | undefined.
connection(#server_info{connection = Connection}) ->
    Connection.

-spec last_write(ServerInfo :: t()) -> primary | secondary | undefined.
last_write(#server_info{last_write = LastWrite}) ->
    LastWrite.

-spec latency(ServerInfo :: t()) -> primary | secondary | undefined.
latency(#server_info{latency = Latency}) ->
    Latency.

-spec update(ServerInfoA :: t(), ServerInfoB :: t()) -> t().
update(#server_info{host = Host, port = Port} = ServerInfoA, #server_info{host = Host, port = Port} = ServerInfoB) ->
    #server_info{role = Role, last_write = LastWrite, latency = Latency} = ServerInfoB,
    ServerInfoA#server_info{role = Role, last_write = LastWrite, latency = Latency};
update(#server_info{} = ServerInfo, #server_info{}) ->
    ServerInfo.

-spec compare(ServerInfoA :: t(), ServerInfoB :: t(), ReadPreference :: tuple()) -> boolean().
compare(#server_info{role = Role} = ServerInfoA, #server_info{role = Role} = ServerInfoB, _) ->
    compare_latency(ServerInfoA, ServerInfoB);
compare(#server_info{} = ServerInfoA, #server_info{} = ServerInfoB, ReadPreference) ->
    case mango_read_preference:mode(ReadPreference) of
        primary -> compare_role(ServerInfoA, ServerInfoB, primary);
        primary_preferred -> compare_role(ServerInfoA, ServerInfoB, primary);
        secondary -> compare_role(ServerInfoA, ServerInfoB, secondary);
        secondary_preferred -> compare_role(ServerInfoA, ServerInfoB, secondary);
        nearest -> compare_latency(ServerInfoA, ServerInfoB)
    end.

-spec compare_role(ServerInfoA :: t(), ServerInfoB :: t(), Preference :: primary | secondary) -> boolean().
compare_role(#server_info{role = Role}, _, Role) -> true;
compare_role(_, #server_info{role = Role}, Role) -> false;
compare_role(#server_info{} = ServerInfoA, #server_info{} = ServerInfoB, _) ->
    compare_latency(ServerInfoA, ServerInfoB).

-spec compare_latency(ServerInfoA :: t(), ServerInfoB :: t()) -> boolean().
compare_latency(#server_info{latency = LatencyA}, #server_info{latency = LatencyB}) ->
    LatencyA =< LatencyB.

-spec is_unknown(ServerInfo :: t()) -> boolean().
is_unknown(#server_info{role = undefined}) -> true;
is_unknown(#server_info{}) -> false.

-spec is_stale(
    ServerInfo :: t(),
    LastWrite :: pos_integer(),
    ReadPreference :: tuple()
) -> boolean().
is_stale(#server_info{last_write = undefined}, _, _) -> true;
is_stale(#server_info{}, undefined, _) -> true;
is_stale(#server_info{last_write = LocalLastWrite}, LastWrite, ReadPreference) ->
    MaxStalenessSeconds = mango_read_preference:max_staleness_seconds(ReadPreference),
    LastWrite - LocalLastWrite > MaxStalenessSeconds * 1000.
