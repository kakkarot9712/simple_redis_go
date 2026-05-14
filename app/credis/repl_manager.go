package credis

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
)

type Repl struct {
	C <-chan bool
}

type ReplClient struct {
	Client
	SigTerm chan<- bool
}

type ReplicaManager interface {
	Processed() uint64
	AppendProcessed(count int)
	Propagator() chan []byte
	Start()
	Stop()
	Add(replica Client) Repl
	Remove(replicaId string)
	NumReplicas() int
	UpdateAcks(replicaId string, acked int64)
	IsReplica(clientId string) bool
	GetAcksFromReplica(minReplica int, bytesProcessed int64) int
	RequestAckFromAllRepl()
}

type replicaManager struct {
	mu         sync.RWMutex
	propagated atomic.Uint64
	propagator chan []byte

	// replicaId => chan
	replicas map[string]*ReplClient

	// replicaId => acked bytes
	acks map[string]int64
}

func (r *replicaManager) UpdateAcks(replicaId string, acked int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks[replicaId] = acked
}

func (r *replicaManager) IsReplica(clientId string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.replicas[clientId] != nil {
		return true
	}
	return false
}

func (r *replicaManager) Propagator() chan []byte {
	return r.propagator
}

func (r *replicaManager) Processed() uint64 {
	return r.propagated.Load()
}

func (r *replicaManager) AppendProcessed(count int) {
	r.propagated.Add(uint64(count))
}

func (r *replicaManager) NumReplicas() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.replicas)
}

func (r *replicaManager) Add(replica Client) Repl {
	r.mu.Lock()
	defer r.mu.Unlock()
	// c := make(chan bool)
	r.replicas[replica.Id()] = &ReplClient{
		Client: replica,
		// SigTerm: c,
	}
	return Repl{
		C: nil,
	}
}

func (r *replicaManager) Remove(replicaId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.replicas[replicaId] != nil {
		// close(r.replicas[replicaId])
		delete(r.replicas, replicaId)
	}
}

func (r *replicaManager) Start() {
	for tkns := range r.propagator {
		r.mu.RLock()
		replicas := r.replicas
		r.mu.RUnlock()
		// propagate to all replica channels
		go func() {
			for _, repl := range replicas {
				repl.Write(tkns)
			}
			r.propagated.Add(uint64(len(tkns)))
		}()
	}
}

func (r *replicaManager) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Println("Stopping Replica Propagation manager...")
	close(r.propagator)
	// for _, r := range r.replicas {
	// 	r.SigTerm <- true
	// }
	fmt.Println("Replica Propagation manager closed")
}

func NewReplManager() ReplicaManager {
	return &replicaManager{
		propagated: atomic.Uint64{},
		propagator: make(chan []byte),
		replicas:   make(map[string]*ReplClient),
		acks:       make(map[string]int64),
	}
}

func (r *replicaManager) RequestAckFromAllRepl() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cmd := []Token{
		NewToken(BULK_STRING, REPLCONF),
		NewToken(BULK_STRING, "GETACK"),
		NewToken(BULK_STRING, "*"),
	}
	for _, repl := range r.replicas {
		repl.Write(NewEncoder().Array(cmd...))
	}
}

func (r *replicaManager) GetAcksFromReplica(minReplica int, bytesProcessed int64) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	replicaAks := 0
	passed := map[string]bool{}
	// fmt.Println(r.acks, "ACKS")
	for _, repl := range r.replicas {
		if r.acks[repl.Id()] >= bytesProcessed {
			replicaAks += 1
			passed[repl.Id()] = true
		}
		if replicaAks >= minReplica {
			break
		}
	}
	return replicaAks
}

func ReplicaCommandGuard(e *executor, req Request, res Response, terminate TerminateFunc) {
	replAllowdCommands := []string{
		REPLCONF,
	}
	if e.deps.ReplicaManager.IsReplica(req.ClientId()) && !slices.Contains(replAllowdCommands, req.Specs().String()) {
		terminate()
		return
	}
}
