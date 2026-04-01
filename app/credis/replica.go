package credis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strings"
)

type replicaConfig struct {
	leaderHost string
	leaderPort int
}

type ReplicaConfigOptions func(*replicaConfig)

func WithLeaderHost(host string) ReplicaConfigOptions {
	return func(cfg *replicaConfig) {
		cfg.leaderHost = host
	}
}

func WithLeaderPort(port int) ReplicaConfigOptions {
	return func(cfg *replicaConfig) {
		cfg.leaderPort = port
	}
}

func (srv *server) StartReplica() {
	if srv.info.Get("replication", "role") != "slave" {
		fmt.Println("won't start slave! server is in master role")
		return
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", srv.replica.leaderHost, srv.replica.leaderPort))
	if err != nil {
		fmt.Printf("failed to connect to master server: %v. aborting.", err)
		os.Exit(1)
	}
	redisClient := NewClient(conn, srv)
	// Handshake 1: Send PING to master
	redisClient.WriteToMaster(PING)
	token, _, err := redisClient.TryParse()
	if !token.IsPong() || err != nil {
		fmt.Println("failed to connect to master server: Handshake 1: Send PING to master. aborting.")
		os.Exit(1)
	}
	// Handshake 2.1: Send REPLCONF listening-port to master
	redisClient.WriteToMaster(
		REPLCONF,
		NewToken(BULK_STRING, "listening-port"),
		NewToken(BULK_STRING, "6380"),
	)
	token, _, err = redisClient.TryParse()
	if !token.IsOk() || err != nil {
		fmt.Println("failed to connect to master server: Handshake 2.1: Send REPLCONF listening-port to master. aborting.")
		os.Exit(1)
	}
	// Handshake 2.2: Send REPLCONF capa to master
	redisClient.WriteToMaster(
		REPLCONF,
		NewToken(BULK_STRING, "capa"),
		NewToken(BULK_STRING, "psync"),
	)
	token, _, err = redisClient.TryParse()
	if !token.IsOk() || err != nil {
		fmt.Println("failed to connect to master server: Handshake 2.2: Send REPLCONF capa to master. aborting.")
		os.Exit(1)
	}
	// Handshake 3: Send PCONF capa to master
	redisClient.WriteToMaster(
		PSYNC,
		NewToken(BULK_STRING, "?"),
		NewToken(BULK_STRING, "-1"),
	)
	token, _, err = redisClient.TryParse()
	if token.Type != SIMPLE_STRING {
		fmt.Println("failed to connect to master server: Handshake 3: Send PCONF capa to master: invalid token type. aborting.")
		os.Exit(1)
	}
	if _, valid := strings.CutPrefix(strings.Trim(token.Literal.(string), "\r\n"), "FULLRESYNC "); !valid || err != nil {
		fmt.Println("failed to connect to master server: Handshake 3: Send PCONF capa to master: invalid token response. aborting.")
		os.Exit(1)
	}
	redisClient.ProcessRDB()
	for {
		token, bytesProcessed, err := redisClient.TryParse()
		if err != nil {
			// TODO: Actual Error
			if errors.Is(err, io.EOF) {
				// Connection is closed
				break
			}
			fmt.Println(err, "rs error")
			continue
		}
		switch token.Type {
		case ARRAY:
			tokens := token.Literal.([]Token)
			exec := NewExec(srv.store, srv.info, srv.rdb)
			if len(tokens) > 0 {
				buffLen := uint(math.Min(float64(2), float64(len(tokens))))
				argsIndex, cmd, err := ParseCmd(tokens[:buffLen]...)
				if err != nil {
					continue
				}
				switch cmd {
				case SET, INCR, REPLCONF:
					req := NewRequest(redisClient, context.TODO())
					var args []Token
					if len(tokens) > argsIndex {
						args = tokens[argsIndex:]
					}
					req.SetArgs(args...)
					out := exec.Exec(req)
					if cmd == REPLCONF {
						conn.Write(out.Data())
					}
				default:
					fmt.Println("command process not allowed for command: ")
				}
			}
		default:
			fmt.Println("unsupported command pattern: ", token)
		}
		// TODO: Validate commands
		redisClient.ProcessedAtomic().Add(uint64(bytesProcessed))
	}
	srv.RemoveFromReplicaGroup(redisClient.Id())
	srv.mu.Lock()
	srv.numReplicas--
	srv.mu.Unlock()
	// Connection ended
}
