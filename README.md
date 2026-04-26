# Simple Redis

This project is a simplified Redis clone implemented in Go, created as part of the CodeCrafters "Build Your Own Redis" challenge. It supports a subset of Redis commands and provides basic functionality for key-value storage and retrieval along with several redis features as described below.

## Supported Features

- Basic key/value storage using `GET` and `SET` command with values with expiry time.
- RDB local database support for persistant storage.
- Partial Replication support.
- List support with `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP` and `BLPOP` commands.
- Sorted sets support with `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE` and `ZREM` commands.
- Streams support with `TYPE` and `XADD` commands.
- Transaction support with `MULTI`, `INCR`, `EXEC`, `DISCARD`, `WATCH` and `UNWATCH` commands.
- Pub/Sub support with `SUBSCRIBE`, `UNSUBSCRIBE` and `PUBLISH` commands.
- Basic ACL support with `AUTH`, `ACL WHOAMI`, `ACL GETUSER` and `ACL SETUSER` commands.
- Geo support with `GEOADD` command.

## Prerequisites

This project requires Go to be installed on your system. If you don't have Go installed, you can download it from the official Go downloads page:

[https://go.dev/dl/](https://go.dev/dl/)

Choose the appropriate version for your operating system and follow the installation instructions provided on the Go website.

## Installation

After ensuring Go is installed on your system, follow these steps to set up Simple Git:

1. Clone the repository:
   ```
   git clone https://github.com/kakkarot9712/simple_redis_go
   ```

2. Navigate to the project directory:
   ```
   cd simple-redis-go
   ```

3. Build the project:
   ```
   go build -o myredis ./app
   ```

This will create an executable named `myredis` in your project directory.

## Usage

- After building the project, you can use the `myredis` executable to start server.
```
./myredis 
```

- By default this server will bind to port `6379`, Though you can use any port using `--port` flag while starting server
```
./myredis --port 6380
```

- You can load existing RDB file while staring the server using `--dir` flag to specify file location and `--dbfilename` flag to specify RDB file name.
```
./myredis --dir /tmp/redis-files --dbfilename dump.rdb
```
**Note**: If you dont specify directory and file paths, server will try to fetch `dump.rdb` file from `/tmp/redis-files` directory.

- By default Server will assume a `master` role. To start server as replica server of some other master server you can pass `--replicaof` flag along with host and port of `master` server.
```
./myredis --replicaof "localhost 6379"
```

## Supported Commands

The following Redis commands are implemented in this project:

1. `PING`: Test the server connection
2. `ECHO`: Echo the given string
3. `SET`: Set a key to hold a string value
4. `GET`: Retrieve the value of a key
5. `CONFIG`: Get or set server configuration parameters
6. `KEYS *`: Find all keys
7. `INFO`: Get information and statistics about the server
8. `REPLCONF`: Configure replication settings
9. `PSYNC`: Internal command used for replication
10. `WAIT`: Wait for replica acknowledgements
11. `TYPE`: Get type of a key (`string`, `stream`, `list`, or `none`)
12. `INCR`: Increments integer value of specified key by 1
13. `MULTI`: Starts a transaction — subsequent commands are queued without execution
14. `EXEC`: Executes all queued commands and returns results as an array
15. `DISCARD`: Discards a previously initialized transaction (with `MULTI`)
16. `XADD`: Append an entry to a stream
17. `RPUSH`: Append one or more values to a list
18. `LPUSH`: Prepend one or more values to a list
19. `LRANGE`: Get a range of elements from a list
20. `LLEN`: Get the length of a list
21. `LPOP`: Remove and return element(s) from the head of a list
22. `BLPOP`: Blocking pop from the head of a list
23. `SUBSCRIBE`: Subscribe to one or more channels
24. `UNSUBSCRIBE`: Unsubscribe from one or more channels
25. `PUBLISH`: Publish a message to a channel
26. `AUTH`: Authenticate with a username and password
27. `ACL WHOAMI`: Return the username of the current connection
28. `ACL GETUSER`: Get flags and passwords for a user
29. `ACL SETUSER`: Create or modify a user (supports `>password` rule to set password)
30. `COMMAND`: Get information about Redis commands
31. `ZADD`: Add one or more members to a sorted set
32. `ZRANK`: Get the rank of a member in a sorted set
33. `ZRANGE`: Get a range of members from a sorted set
34. `ZCARD`: Get the cardinality (number of members) of a sorted set
35. `ZSCORE`: Get the score of a member in a sorted set
36. `ZREM`: Remove one or more members from a sorted set
37. `WATCH`: Watch one or more keys for transaction
38. `UNWATCH`: Unwatch all keys
39. `GEOADD`: Add one or more locations to a geo key
40. `QUIT`: Close the connection

## Limitations

- `HGET` and `HSET` commands are not supported.
- `XRANGE` and `XREAD` stream commands are not supported; only `XADD` is available.
- `PSUBSCRIBE` and `PUNSUBSCRIBE` (pattern-based pub/sub) are not supported.
- RDB file loading is supported but `SAVE` command (writing RDB) is not.
- Only RDB with single database and basic key-value storage is supported.
- ACL support is limited to password-based authentication; command/key permissions are not enforced.

## Acknowledgments

- CodeCrafters for providing the "Build Your Own Redis" challenge
- The Redis project for inspiration and documentation
- HDT3213 for CRC64 Checksum [Jones](https://github.com/HDT3213/rdb/blob/e5a00e130dda889ce1396d5561f95540418d12fc/crc64jones/crc64.go) varient implimentation.