package server

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/credis"
	"github.com/codecrafters-io/redis-starter-go/app/credis/executor"
	"github.com/codecrafters-io/redis-starter-go/app/credis/helpers"
	"github.com/codecrafters-io/redis-starter-go/app/credis/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/parser"
	"github.com/codecrafters-io/redis-starter-go/app/credis/store"
)

type ConnectionHandler interface {
	SendConn(conn net.Conn, parser credis.ParserProvider, exec executor.Executor, replHandler credis.ReplicaHandler)
}

type config struct {
	port        int
	host        string
	slave       *slaveConfig
	rdbDir      string
	rdbFileName string
}

type ConfigOption func(*config)

func WithPort(port int) ConfigOption {
	return func(opts *config) {
		opts.port = port
	}
}

func WithHost(host string) ConfigOption {
	return func(opts *config) {
		opts.host = host
	}
}

func WithSlave(slvOpts ...SlaveConfigOptions) ConfigOption {
	return func(opts *config) {
		slvcfg := slaveConfig{}
		for _, opt := range slvOpts {
			opt(&slvcfg)
		}
		opts.slave = &slvcfg
	}
}

type Server struct {
	mu        sync.RWMutex
	port      int
	host      string
	slave     *slaveConfig
	info      ServerInfo
	handler   ConnectionHandler
	BaseStore store.BaseStore
	replicas  map[string]*credis.Conn
	rdb       rdb.RDBStore
}

func New(handler ConnectionHandler, opts ...ConfigOption) *Server {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	srv := Server{
		BaseStore: store.New(),
		handler:   handler,
		host:      "0.0.0.0",
		port:      6379,
		info:      NewInfo(),
	}
	if cfg.host != "" {
		srv.host = cfg.host
	}
	if cfg.port != 0 {
		srv.port = cfg.port
	}
	if cfg.slave != nil {
		srv.slave = cfg.slave
		srv.info.set("replication", "role", "slave")
	} else {
		srv.replicas = make(map[string]*credis.Conn)
		srv.info.set("replication", "role", "master")
		srv.info.set("replication", "master_repl_offset", "0")
		srv.info.set("replication", "master_replid", helpers.GenerateString(40))
	}
	if cfg.rdbDir != "" && cfg.rdbFileName != "" {
		srv.rdb = rdb.New(cfg.rdbDir, cfg.rdbFileName)
		srv.rdb.Load()
		if srv.rdb.Error() != nil {
			fmt.Printf("RDB Restore aborted: %v", srv.rdb.Error().Error())
		} else {
			srv.rdb.Restore(srv.BaseStore)
		}
	}
	return &srv
}

func (srv *Server) StartMaster() error {
	l, err := net.Listen("tcp", fmt.Sprintf("%v:%v", srv.host, srv.port))
	if err != nil {
		return fmt.Errorf("failed to bind to port %v", srv.port)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("error accepting connection: %v", err.Error())
		}
		parser := parser.New(bufio.NewReader(conn))
		exec := executor.New(srv.BaseStore, &srv.info, srv.rdb)

		srv.handler.SendConn(conn, parser, exec, srv)
	}
}

func (srv *Server) Info() ServerInfo {
	return srv.info
}
