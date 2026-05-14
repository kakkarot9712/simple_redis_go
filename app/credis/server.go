package credis

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

type SectionInfo map[string]string

type serverInfo map[string]SectionInfo

type ServerInfo interface {
	Get(section string, key string) string
	Section(section string) map[string]string
	IsSlave() bool
}

func NewInfo() *serverInfo {
	return &serverInfo{
		"replication": make(SectionInfo),
		"persistence": make(SectionInfo),
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

func (info *serverInfo) IsSlave() bool {
	return info.Get("replication", "role") == "slave"
}

type Server interface {
	GetReplicaNums() uint
	SubscribeToReplicaUpdates(c Client) *ReplicaUpdateSubscription
	Hub() Hub
	AddToReplicaGroup(id string, conn io.Writer)
	PropagateToReplicaGroup(cmd string, args ...Token)
	IsPartOfReplicaGroup(id string) bool
	RemoveFromReplicaGroup(id string)
	StartMaster() error
	StartReplica(flags *Flags, d *deps)
	Dir() string
	Shutdown()
}

type server struct {
	mu                          sync.RWMutex
	port                        int
	host                        string
	replica                     *replicaConfig
	numReplicas                 uint
	replicaUpdatesSubscriptions map[string]chan uint
	hub                         Hub
	replicas                    map[string]io.Writer
	dir                         string
}

func New(hub Hub, flgs *Flags) Server {
	// defaultAuth := DefaultAuth()
	srv := &server{
		hub:                         hub,
		host:                        "0.0.0.0",
		port:                        6379,
		replicaUpdatesSubscriptions: make(map[string]chan uint),
		dir:                         flgs.Dir,
	}
	if flgs.Host != "" {
		srv.host = flgs.Host
	}
	if flgs.Port != 0 {
		srv.port = flgs.Port
	}
	if flgs.ReplicaOf != "" {
		replicaConnInfo := strings.Split(flgs.ReplicaOf, " ")
		if len(replicaConnInfo) != 2 {
			fmt.Println("Invalid args passed for --replicaof")
			os.Exit(1)
		}
		masterHost := replicaConnInfo[0]
		masterPort, err := strconv.ParseUint(replicaConnInfo[1], 10, 64)
		if err != nil {
			fmt.Println("Invalid args passed for --replicaof")
			os.Exit(1)
		}
		srv.replica = &replicaConfig{
			leaderHost: masterHost,
			leaderPort: int(masterPort),
		}
	} else {
		srv.replicas = make(map[string]io.Writer)
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
		go handle(NewClient(conn, srv.hub.RequestChannel(), DefaultAuthContext().user, false))
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

func (srv *server) Dir() string {
	return srv.dir
}

func (srv *server) Shutdown() {}
