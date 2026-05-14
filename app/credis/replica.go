package credis

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"slices"
	"strings"
	"time"
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

func RestrictUnsupportedCommandMiddleware(e *executor, req Request, res Response) error {
	supportedCommands := []string{
		SET,
		INCR,
		LPOP,
		GEOADD,
		LPUSH,
		XADD,
		ZADD,
		REPLCONF,
	}
	if !slices.Contains(supportedCommands, req.Specs().String()) {
		return &ErrCommandNotPropagateble{
			cmd: req.Specs().String(),
		}
	}
	return nil
}

func (srv *server) StartReplica(flags *Flags, d *deps) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", srv.replica.leaderHost, srv.replica.leaderPort))
	if err != nil {
		fmt.Printf("failed to connect to master server: %v. aborting.", err)
		os.Exit(1)
	}
	redisClient := NewClient(conn, srv.hub.RequestChannel(), DefaultAuthContext().user, false)
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
	exec := NewExec(d)
	exec.Use(ExecutorMiddleware)
	// exec.Use(RestrictUnsupportedCommandMiddleware)
	go handleRepl(redisClient, exec, d.ReplicaManager)
}

func handleRepl(client Client, exec Executor, repl ReplicaManager) {
	clientCtx, clientCancel := context.WithCancel(context.Background())
	timer := time.NewTicker(time.Second)
	type masterData struct {
		bytes int
		token Token
	}
	masterResp := make(chan masterData)
	go func() {
		for {
			token, bytesProcessed, err := client.TryParse()
			if err != nil {
				// Actual Error
				close(masterResp)
				break
			}
			masterResp <- masterData{
				bytes: bytesProcessed,
				token: token,
			}
		}
	}()
l:
	for {
		select {
		case <-timer.C:
			// client.WriteToMaster(REPLCONF,
			// 	NewToken(BULK_STRING, "ACK"),
			// 	NewToken(BULK_STRING, fmt.Sprintf("%v", repl.Processed())),
			// )
		case md, ok := <-masterResp:
			if !ok {
				break l
			}
			token := md.token
			tokenType := token.Type
			if tokenType != ARRAY {
				// Ignore that as of now
				continue
			}
			tkns := token.Literal.([]Token)
			if len(tkns) == 0 {
				continue
			}
			// var artifacts any
			reqCtx, cancel := context.WithCancel(clientCtx)
			sendAndCancel := func(res Response) {
				client.Write(res.Data())
				// artifacts = res.Artifacts()
				cancel()
			}
			buffLen := uint(math.Min(float64(2), float64(len(tkns))))
			argsIndex, cmd, err := ParseCmd(tkns[:buffLen]...)
			var args []Token
			if len(tkns) > argsIndex {
				args = tkns[argsIndex:]
			}
			tx := NewTX()
			req := NewRequest(
				reqCtx,
				client.AuthCtx(),
				tx,
				client,
				nil,
			)
			specs, err := ParseSpec(cmd, args...)
			req.SetSpecs(specs)
			if err != nil {
				sendAndCancel(&response{
					data: NewEncoder().SimpleError(err.Error()),
				})
				continue
			}
			if len(tkns) > 1 {
				args = append(args, tkns[1:]...)
			}
			req.SetArgs(args...)
			if Writeable(cmd) {
				res := exec.Exec(req)
				if cmd == REPLCONF {
					sendAndCancel(res)
				}
			}
			repl.AppendProcessed(md.bytes)
			cancel()
		}
	}
	clientCancel()
}
