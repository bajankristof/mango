%% @hidden
-module(mango_topology_info).

-export([
    new/1,
    database/1,
    read_preference/1,
    retry_reads/1,
    retry_writes/1,
    servers/1,
    add_server/2,
    update_server/2,
    select_server/2,
    select_primary/1,
    select_secondary/1,
    select_nearest/1,
    sort_servers/1,
    last_write/1,
    should_retry/2,
    prepare/2
]).

-include("mango.hrl").
-include("topology.hrl").

-type t() :: #topology_info{}.
-type server_info() :: #server_info{}.

-spec new(Opts :: mango:start_opts()) -> t().
new(#{database := Database} = Opts) when Database =/= '$' ->
    #topology_info{database = Database, read_preference = mango_read_preference:new(Opts)}.

-spec database(TopologyInfo :: t()) -> mango:database().
database(#topology_info{database = Database}) ->
    Database.

-spec read_preference(TopologyInfo :: t()) -> tuple().
read_preference(#topology_info{read_preference = ReadPreference}) ->
    ReadPreference.

-spec retry_reads(TopologyInfo :: t()) -> boolean().
retry_reads(#topology_info{retry_reads = RetryReads}) ->
    RetryReads.

-spec retry_writes(TopologyInfo :: t()) -> boolean().
retry_writes(#topology_info{retry_writes = RetryWrites}) ->
    RetryWrites.

-spec servers(TopologyInfo :: t()) -> [server_info()].
servers(#topology_info{servers = Servers}) ->
    Servers.

-spec add_server(TopologyInfo :: t(), ServerInfo :: server_info()) -> t().
add_server(#topology_info{servers = Servers} = TopologyInfo, #server_info{} = ServerInfo) ->
    TopologyInfo#topology_info{servers = [ServerInfo | Servers]}.

-spec update_server(TopologyInfo :: t(), ServerInfo :: server_info()) -> t().
update_server(#topology_info{servers = Servers0} = TopologyInfo, #server_info{} = ServerInfo) ->
    Servers = lists:map(fun (LocalServerInfo) ->
        mango_server_info:update(LocalServerInfo, ServerInfo)
    end, Servers0),
    sort_servers(TopologyInfo#topology_info{servers = Servers}).

-spec select_server(TopologyInfo :: t(), Operation :: read | write) -> {value, server_info()} | false.
select_server(#topology_info{servers = [Server]}, _) ->
    {value, Server};
select_server(#topology_info{servers = [_|_]} = TopologyInfo, write) ->
    select_primary(TopologyInfo);
select_server(#topology_info{servers = [_|_], read_preference = ReadPreference} = TopologyInfo, read) ->
    case mango_read_preference:mode(ReadPreference) of
        primary -> select_primary(TopologyInfo);
        secondary -> select_secondary(TopologyInfo);
        _ -> select_nearest(TopologyInfo)
    end.

-spec select_primary(TopologyInfo :: t()) -> {value, server_info()} | false.
select_primary(#topology_info{servers = Servers}) ->
    lists:search(fun (ServerInfo) ->
        mango_server_info:role(ServerInfo) =:= primary
    end, Servers).

-spec select_secondary(TopologyInfo :: t()) -> {value, server_info()} | false.
select_secondary(#topology_info{servers = [_|_] = Servers, read_preference = ReadPreference} = TopologyInfo) ->
    LastWrite = last_write(TopologyInfo),
    lists:search(fun (ServerInfo) ->
        mango_server_info:role(ServerInfo) =:= secondary
        andalso not mango_server_info:is_stale(ServerInfo, LastWrite, ReadPreference)
    end, Servers).

-spec select_nearest(TopologyInfo :: t()) -> {value, server_info()} | false.
select_nearest(#topology_info{servers = [_|_] = Servers, read_preference = ReadPreference} = TopologyInfo) ->
    LastWrite = last_write(TopologyInfo),
    lists:search(fun (ServerInfo) ->
        not mango_server_info:is_unknown(ServerInfo)
        andalso not mango_server_info:is_stale(ServerInfo, LastWrite, ReadPreference)
    end, Servers).

-spec sort_servers(TopologyInfo :: t()) -> t().
sort_servers(#topology_info{servers = Servers0, read_preference = ReadPreference} = TopologyInfo) ->
    Servers = lists:sort(fun (ServerInfoA, ServerInfoB) ->
        mango_server_info:compare(ServerInfoA, ServerInfoB, ReadPreference)
    end, Servers0),
    TopologyInfo#topology_info{servers = Servers}.

-spec last_write(TopologyInfo :: t()) -> non_neg_integer().
last_write(#topology_info{servers = Servers}) ->
    lists:foldl(fun (ServerInfo, Acc) ->
        case mango_server_info:last_write(ServerInfo) of
            undefined -> Acc;
            LastWrite when LastWrite < Acc -> Acc;
            LastWrite -> LastWrite
        end
    end, 0, Servers).

-spec should_retry(TopologyInfo :: t(), Command :: mango:command()) -> boolean().
should_retry(TopologyInfo, #command{type = read}) ->
    retry_reads(TopologyInfo);
should_retry(TopologyInfo, _) ->
    retry_writes(TopologyInfo).

-spec prepare(TopologyInfo :: t(), Command :: mango:command()) -> mango:command().
prepare(#topology_info{database = Database} = TopologyInfo, #command{database = '$'} = Command) ->
    prepare(TopologyInfo, Command#command{database = Database});
prepare(#topology_info{read_preference = ReadPreference}, #command{type = read, opts = Opts} = Command) ->
    Command#command{opts = [{<<"$readPreference">>, mango_read_preference:to_map(ReadPreference)} | Opts]};
prepare(_, #command{} = Command) -> Command.
