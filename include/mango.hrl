-record('mango.command', {type = write, command, database = '$', opts = []}).
-record('mango.cursor', {id, connection, database, collection, first_batch, opts = []}).
