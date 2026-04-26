package credis

import (
	"fmt"
	"io"
	"net"
	"sync"
)

type SectionInfo map[string]string

type serverInfo map[string]SectionInfo

type ServerInfo interface {
	Get(section string, key string) string
	Section(section string) map[string]string
}

func NewInfo() *serverInfo {
	return &serverInfo{
		"replication": make(SectionInfo),
	}
}

func (info *serverInfo) Get(section string, key string) string {
	return (*info)[section][key]
}

func (info *serverInfo) Section(section string) map[string]string {
	return (*info)[section]
}

func (info *serverInfo) set(section string, key string, value string) {
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

type Server interface {
	GetReplicaNums() uint
	SubscribeToReplicaUpdates(c Client) *ReplicaUpdateSubscription
	SubManager() Subscription
	Hub() Hub
	Store() dataStores
	Info() ServerInfo
	RDB() RDBStore
	AddToReplicaGroup(id string, conn io.Writer)
	PropagateToReplicaGroup(cmd string, args ...Token)
	IsPartOfReplicaGroup(id string) bool
	RemoveFromReplicaGroup(id string)
	StartMaster() error
	StartReplica()
	Auth(user string) Auth
}

type dataStores struct {
	KV     KVStore
	Stream Stream
	List   ListStore[string]
}

type server struct {
	mu                          sync.RWMutex
	port                        int
	host                        string
	replica                     *replicaConfig
	numReplicas                 uint
	replicaUpdatesSubscriptions map[string]chan uint
	info                        *serverInfo
	hub                         Hub
	subManager                  Subscription
	store                       dataStores
	replicas                    map[string]io.Writer
	rdb                         RDBStore
	auth                        map[string]Auth
}

func New(hub Hub, opts ...ConfigOption) Server {
	cfg := config{}
	defaultAuth := DefaultAuth()
	for _, opt := range opts {
		opt(&cfg)
	}
	srv := &server{
		store: dataStores{
			NewStore(), NewStream(), NewListStore[string](),
		},
		hub:                         hub,
		host:                        "0.0.0.0",
		port:                        6379,
		replicaUpdatesSubscriptions: make(map[string]chan uint),
		info:                        NewInfo(),
		subManager:                  NewSubscriptionManager(),
		auth: map[string]Auth{
			defaultAuth.User(): defaultAuth,
		},
	}
	if cfg.host != "" {
		srv.host = cfg.host
	}
	if cfg.port != 0 {
		srv.port = cfg.port
	}
	if cfg.replica != nil {
		srv.replica = cfg.replica
		srv.info.set("replication", "role", "slave")
	} else {
		srv.replicas = make(map[string]io.Writer)
		srv.info.set("replication", "role", "master")
		srv.info.set("replication", "master_repl_offset", "0")
		srv.info.set("replication", "master_replid", GenerateString(40))
	}
	if cfg.rdbDir != "" && cfg.rdbFileName != "" {
		srv.rdb = NewRDB(cfg.rdbDir, cfg.rdbFileName)
		srv.rdb.Load()
		if srv.rdb.Error() != nil {
			fmt.Printf("RDB Restore aborted: %v", srv.rdb.Error().Error())
		} else {
			srv.rdb.Restore(srv.store.KV)
		}
	}
	return srv
}

func (srv *server) StartMaster() error {
	l, err := net.Listen("tcp", fmt.Sprintf("%v:%v", srv.host, srv.port))
	if err != nil {
		return fmt.Errorf("failed to bind to port %v", srv.port)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("error accepting connection: %v", err.Error())
		}
		go handle(NewClient(conn, srv))
	}
}

func (srv *server) AddToReplicaGroup(id string, writer io.Writer) {
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

func (srv *server) PropagateToReplicaGroup(cmd string, args ...Token) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	for _, repl := range srv.replicas {
		enc := NewEncoder()
		tkns := []Token{
			NewToken(BULK_STRING, cmd),
		}
		tkns = append(tkns, args...)
		repl.Write(enc.Array(tkns...))
	}
}

func (srv *server) RemoveFromReplicaGroup(id string) {
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

func (srv *server) IsPartOfReplicaGroup(id string) bool {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	return srv.replicas[id] != nil
}

func (srv *server) Hub() Hub {
	return srv.hub
}

func (srv *server) Auth(user string) Auth {
	return srv.auth[user]
}

type ReplicaUpdateSubscription struct {
	C      <-chan uint
	cancel func()
	id     string
}

func (srv *server) SubscribeToReplicaUpdates(c Client) *ReplicaUpdateSubscription {
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

func (srv *server) GetReplicaNums() uint {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.numReplicas
}

func (srv *server) SubManager() Subscription {
	return srv.subManager
}

func (srv *server) Store() dataStores {
	return srv.store
}

func (srv *server) Info() ServerInfo {
	return srv.info
}

func (srv *server) RDB() RDBStore {
	return srv.rdb
}
