-record(topology_info, {
    type,
    database,
    read_preference,
    read_from,
    write_to,
    retry_reads = true,
    retry_writes = false,
    servers = []
}).
-record(server_info, {
    role,
    host,
    port,
    connection,
    last_write,
    latency
}).
