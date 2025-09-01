package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/app/credis/client"
	"github.com/codecrafters-io/redis-starter-go/app/credis/commands"
	"github.com/codecrafters-io/redis-starter-go/app/credis/executor"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"
)

type slaveConfig struct {
	masterHost string
	masterPort int
}

type SlaveConfigOptions func(*slaveConfig)

func WithMasterHost(host string) SlaveConfigOptions {
	return func(cfg *slaveConfig) {
		cfg.masterHost = host
	}
}

func WithMasterPort(port int) SlaveConfigOptions {
	return func(cfg *slaveConfig) {
		cfg.masterPort = port
	}
}

func (srv *Server) StartSlave() {
	var processedByte atomic.Uint64
	if srv.info.Get("replication", "role") != "slave" {
		fmt.Println("won't start slave! server is in master role")
		return
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", srv.slave.masterHost, srv.slave.masterPort))
	if err != nil {
		fmt.Printf("failed to connect to master server: %v. aborting.", err)
		os.Exit(1)
	}
	redisClient := client.New(conn)
	// Handshake 1: Send PING to master
	redisClient.Send(commands.PING)
	token, _ := redisClient.TryParse()
	if !token.IsPong() {
		fmt.Println("failed to connect to master server: Handshake 1: Send PING to master. aborting.")
		os.Exit(1)
	}
	// Handshake 2.1: Send REPLCONF listening-port to master
	redisClient.Send(
		commands.REPLCONF,
		tokens.New(types.BULK_STRING, "listening-port"),
		tokens.New(types.BULK_STRING, "6380"),
	)
	token, _ = redisClient.TryParse()
	if !token.IsOk() {
		fmt.Println("failed to connect to master server: Handshake 2.1: Send REPLCONF listening-port to master. aborting.")
		os.Exit(1)
	}
	// Handshake 2.2: Send REPLCONF capa to master
	redisClient.Send(
		commands.REPLCONF,
		tokens.New(types.BULK_STRING, "capa"),
		tokens.New(types.BULK_STRING, "psync"),
	)
	token, _ = redisClient.TryParse()
	if !token.IsOk() {
		fmt.Println("failed to connect to master server: Handshake 2.2: Send REPLCONF capa to master. aborting.")
		os.Exit(1)
	}
	// Handshake 3: Send PCONF capa to master
	redisClient.Send(
		commands.PSYNC,
		tokens.New(types.BULK_STRING, "?"),
		tokens.New(types.BULK_STRING, "-1"),
	)
	token, _ = redisClient.TryParse()
	if token.Type != types.SIMPLE_STRING {
		fmt.Println("failed to connect to master server: Handshake 3: Send PCONF capa to master: invalid token type. aborting.")
		os.Exit(1)
	}
	if _, valid := strings.CutPrefix(strings.Trim(token.Literal.(string), "\r\n"), "FULLRESYNC "); !valid {
		fmt.Println("failed to connect to master server: Handshake 3: Send PCONF capa to master: invalid token response. aborting.")
		os.Exit(1)
	}
	redisClient.ProcessRDB()
	exec := executor.New(srv.BaseStore, &srv.info, srv.rdb)
	for {
		token, bytesProcessed := redisClient.TryParse()
		if redisClient.Error() != nil {
			// TODO: Actual Error
			if errors.Is(redisClient.Error(), io.EOF) {
				// Connection is closed
				break
			}
			fmt.Println(redisClient.Error(), "rs error")
			continue
		}
		switch token.Type {
		case types.ARRAY:
			tokens := token.Literal.([]tokens.Token)
			if len(tokens) > 0 {
				command := tokens[0]
				c := strings.ToLower(command.Literal.(string))
				switch c {
				case commands.SET, commands.INCR, commands.REPLCONF:
					exec.Exec(
						c,
						tokens[1:],
						executor.WithProcessedBytesAtomic(&processedByte),
					)
					if exec.Error() != nil {
						// TODO: Something is wrong
						fmt.Println("error while execution: ", exec.Error())
					}
					if c == commands.REPLCONF {
						conn.Write(exec.Out())
					}
				default:
					fmt.Println("command process not allowed for command: ", c)
				}
			}
		default:
			fmt.Println("unsupported command pattern: ", token)
		}
		fmt.Println("SUM", bytesProcessed)
		// TODO: Validate commands
		processedByte.Add(uint64(bytesProcessed))
	}
	// Connection ended
}
