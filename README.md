# Simple Redis

This project is a simplified Redis clone implemented in Go, created as part of the CodeCrafters "Build Your Own Redis" challenge. It supports a subset of Redis commands and provides basic functionality for key-value storage and retrieval.

## Supported Features

- Basic key/value storage using `GET` and `SET` command with values with expiry time.
- RDB local database support for persistant storage.
- Partial Replication support.
- Basic streams support with `TYPE`, `XADD`, `XREAD` and `XRANGE` commands.

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
10. `TYPE`: Get type of key, which can be `string`, `stream` or `none`
11. `XADD`: Add key-value(s) to a streams storage
12. `XRANGE`: Get all streams within two id range
13. `XREAD`: Read multiple streams of specific ids. supports normal lookup and `blocking` lookup

## Limitations

- This server does not support `HGET` and `HSET` command as of now.
- Replication is partially supported. Though propagation of commands does works, some commands related to command propagation valdation like `WAIT` is not implimented as of now.
- Server only supports loading RDB file from specified location but does not support RDB File saving using `SAVE` command as of now.
- This server only supports very limited and basic features.

## Acknowledgments

- CodeCrafters for providing the "Build Your Own Redis" challenge
- The Redis project for inspiration and documentation