%% @hidden
-module(mango_url).

-export([
    parse/1,
    parse_hosts/1,
    parse_host/1,
    parse_port/1
]).

-define(URL_REGEX, "^(?:mongodb(?:\\+srv)?):\\/\\/?(?<hosts>[^\\/]+)(\\/(?<database>[^\\?]+))?$").

%% === API Functions ===

-spec parse(URL :: iolist()) ->
    #{hosts := [mango:hostent()]} |
    #{hosts := [mango:hostent()], database := mango:database()}.
parse(URL) ->
    case re:run(URL, ?URL_REGEX, [{capture, [hosts, database], list}]) of
        {match, [Hosts, []]} -> #{hosts => parse_hosts(Hosts)};
        {match, [Hosts, Database]} -> #{hosts => parse_hosts(Hosts), database => Database}
    end.

-spec parse_hosts(Hosts :: list()) -> [mango:hostent()].
parse_hosts(Hosts) when erlang:is_list(Hosts) ->
    lists:map(fun parse_host/1, string:split(Hosts, ",", all)).

-spec parse_host(Host :: list()) -> mango:hostent().
parse_host(Host) when erlang:is_list(Host) ->
    case string:split(Host, ":") of
        [Hostname, Port] -> {Hostname, parse_port(Port)};
        [Hostname] -> Hostname
    end.

-spec parse_port(Port :: list()) -> inet:port_number().
parse_port(Port) when erlang:is_list(Port) ->
    erlang:list_to_integer(Port).
