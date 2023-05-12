-record(command, {
    type = write :: read | write,
    command :: {binary(), term()},
    database = '$' :: '$' | mango:database(),
    opts = [] :: [{binary(), term()}]
}).
-record(cursor, {
    id :: non_neg_integer(),
    connection :: pid(),
    database :: mango:database(),
    collection :: mango:collection(),
    first_batch :: [bson:document()],
    opts = [] :: [{binary(), term()}]
}).
