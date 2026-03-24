package credis

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
)

type SectionInfo map[string]string

type ServerInfo map[string]SectionInfo

func NewInfo() ServerInfo {
	return ServerInfo{
		"replication": make(SectionInfo),
	}
}

func (info *ServerInfo) Get(section string, key string) string {
	return (*info)[section][key]
}

func (info *ServerInfo) Section(section string) map[string]string {
	return (*info)[section]
}

func (info *ServerInfo) set(section string, key string, value string) {
	(*info)[section][key] = value
}

type config struct {
	port        int
	host        string
	replica     *replicaConfig
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

func AsReplica(replOpts ...ReplicaConfigOptions) ConfigOption {
	return func(opts *config) {
		cfg := replicaConfig{}
		for _, opt := range replOpts {
			opt(&cfg)
		}
		opts.replica = &cfg
	}
}

type Server struct {
	mu                          sync.RWMutex
	port                        int
	host                        string
	replica                     *replicaConfig
	numReplicas                 uint
	replicaUpdatesSubscriptions map[string]chan uint
	Info                        ServerInfo
	hub                         *Hub
	Store                       struct {
		KV     KVStore
		Stream Stream
		List   ListStore[string]
	}
	replicas map[string]io.Writer
	Rdb      RDBStore
}

func New(hub *Hub, opts ...ConfigOption) *Server {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	srv := Server{
		Store: struct {
			KV     KVStore
			Stream Stream
			List   ListStore[string]
		}{
			NewStore(), NewStream(), NewListStore[string](),
		},
		hub:                         hub,
		host:                        "0.0.0.0",
		port:                        6379,
		replicaUpdatesSubscriptions: make(map[string]chan uint),
		Info:                        NewInfo(),
	}
	if cfg.host != "" {
		srv.host = cfg.host
	}
	if cfg.port != 0 {
		srv.port = cfg.port
	}
	if cfg.replica != nil {
		srv.replica = cfg.replica
		srv.Info.set("replication", "role", "slave")
	} else {
		srv.replicas = make(map[string]io.Writer)
		srv.Info.set("replication", "role", "master")
		srv.Info.set("replication", "master_repl_offset", "0")
		srv.Info.set("replication", "master_replid", GenerateString(40))
	}
	if cfg.rdbDir != "" && cfg.rdbFileName != "" {
		srv.Rdb = NewRDB(cfg.rdbDir, cfg.rdbFileName)
		srv.Rdb.Load()
		if srv.Rdb.Error() != nil {
			fmt.Printf("RDB Restore aborted: %v", srv.Rdb.Error().Error())
		} else {
			srv.Rdb.Restore(srv.Store.KV)
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
		parser := NewParser(bufio.NewReader(conn))
		id := GenerateString(10)
		go handle(
			&client{
				id:      id,
				Conn:    conn,
				parser:  parser,
				Srv:     srv,
				receive: make(chan Response),
				send:    srv.hub.RequestChan,
				tx:      NewTX(),
				sub:     NewSubscriptionProvider(id),
				exec:    srv.hub.executor,
			},
		)
	}
}

func (srv *Server) AddToReplicaGroup(id string, writer io.Writer) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.replicas[id] = writer
	srv.numReplicas++

	go func() {
		// Notifiy to subscribers with new replica numbers
		for _, sub := range srv.replicaUpdatesSubscriptions {
			sub <- srv.numReplicas
		}
	}()
}

func (srv *Server) PropagateToReplicaGroup(cmd string, args ...Token) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	for _, repl := range srv.replicas {
		enc := NewEncoder()
		tkns := []Token{
			NewToken(BULK_STRING, cmd),
		}
		tkns = append(tkns, args...)
		enc.Array(tkns...)
		enc.Commit()
		repl.Write(enc.Bytes())
	}
}

func (srv *Server) RemoveFromReplicaGroup(id string) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	delete(srv.replicas, id)
	srv.numReplicas--

	go func() {
		// Notifiy to subscribers with new replica numbers
		for _, sub := range srv.replicaUpdatesSubscriptions {
			sub <- srv.numReplicas
		}
	}()
}

func (srv *Server) IsPartOfReplicaGroup(id string) bool {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	return srv.replicas[id] != nil
}

type ReplicaUpdateSubscription struct {
	C      <-chan uint
	cancel func()
	id     string
}

func (srv *Server) SubscribeToReplicaUpdates(c Client) *ReplicaUpdateSubscription {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	subC := make(chan uint)
	sub := ReplicaUpdateSubscription{
		C:  subC,
		id: c.Id(),
	}
	sub.cancel = func() {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		delete(srv.replicaUpdatesSubscriptions, sub.id)
	}
	srv.replicaUpdatesSubscriptions[sub.id] = subC
	return &sub
}

func (srv *Server) GetReplicaNums() uint {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.numReplicas
}
