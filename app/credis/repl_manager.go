package credis

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Repl struct {
	C <-chan []byte
}

type ReplicaManager interface {
	Processed() uint64
	AppendProcessed(count int)
	Propagator() chan []byte
	Start()
	Stop()
	Add(replicaId string) Repl
	Remove(replicaId string)
	NumReplicas() int
}

type replicaManager struct {
	mu         sync.RWMutex
	propagated atomic.Uint64
	propagator chan []byte

	// replicaId => chan
	replicas map[string]chan []byte
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

func (r *replicaManager) Add(replicaId string) Repl {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := make(chan []byte)
	fmt.Println(replicaId, "REPLID")
	r.replicas[replicaId] = c
	return Repl{
		C: c,
	}
}

func (r *replicaManager) Remove(replicaId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.replicas[replicaId] != nil {
		close(r.replicas[replicaId])
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
				repl <- tkns
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
}

func NewReplManager() ReplicaManager {
	return &replicaManager{
		propagated: atomic.Uint64{},
		propagator: make(chan []byte),
		replicas:   map[string]chan []byte{},
	}
}
