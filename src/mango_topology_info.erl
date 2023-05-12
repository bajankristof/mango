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
    update_server/2
]).

-include("topology.hrl").

-type t() :: #topology_info{}.
-type server_info() :: #server_info{}.

-spec new(Opts :: mango:start_opts()) -> t().
new(#{database := Database} = Opts) ->
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
    TopologyInfo#topology_info{servers = Servers}.
