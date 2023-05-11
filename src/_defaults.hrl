-define(DEFAULT_TIMEOUT, 60_000).

-define(DEFAULT_HOST, "127.0.0.1").
-define(DEFAULT_PORT, 27017).
-define(DEFAULT_POOL_SIZE, 10).
-define(DEFAULT_MIN_BACKOFF, 1_000).
-define(DEFAULT_MAX_BACKOFF, 30_000).
-define(DEFAULT_MAX_ATTEMPTS, infinity).
-define(DEFAULT_READ_PREFERENCE, primary).
-define(DEFAULT_RETRY_READS, true).
-define(DEFAULT_RETRY_WRITES, true).

-define(DEFAULT_OPTS, #{
    pool_size => ?DEFAULT_POOL_SIZE,
    min_backoff => ?DEFAULT_MIN_BACKOFF,
    max_backoff => ?DEFAULT_MAX_BACKOFF,
    max_attempts => ?DEFAULT_MAX_ATTEMPTS,
    read_preference => ?DEFAULT_READ_PREFERENCE,
    retry_reads => ?DEFAULT_RETRY_READS,
    retry_writes => ?DEFAULT_RETRY_WRITES
}).
