package credis

import (
	"fmt"
	"io"
	"sync"
	"time"
)

const WORKERS_LIMIT = 6

type WaitingArea struct {
	queue map[string][]BLPOPHold
	mu    sync.Mutex
}

var keyUpdatesChan = make(chan string, WORKERS_LIMIT)

var waitingArea = WaitingArea{
	queue: make(map[string][]BLPOPHold),
}

type Hub struct {
	RequestChan chan Request
	wg          sync.WaitGroup
	executor    Executor
	replHandler ReplicaHandler
}

type ReplicaHandler interface {
	AddToReplicaGroup(id string, conn io.Writer)
	PropagateToReplicaGroup(cmd string, args ...Token)
	IsPartOfReplicaGroup(id string) bool
	RemoveFromReplicaGroup(id string)
}

func NewHub() *Hub {
	handler := make(chan Request, WORKERS_LIMIT)
	return &Hub{
		RequestChan: handler,
	}
}

func (h *Hub) StartWorker() {
	h.wg.Add(1)
	for {
		select {
		case req, ok := <-h.RequestChan:
			if !ok {
				h.wg.Done()
				break
			}
			out := h.executor.Exec(req)
			if h.executor.Error() != nil {
				// Something is wrong
				fmt.Println("error while execution: ", h.executor.Error())
			}
			spec := req.Cmd()
			if spec, ok := spec.(*BLPOPSpecs); ok && !spec.Concluded {
				continue
			}
			req.Receive() <- &response{
				data: out,
			}
			// Propagate to replicas
			cmd := req.Cmd().String()
			args := req.Args()
			switch cmd {
			case SET, INCR:
				h.replHandler.PropagateToReplicaGroup(cmd, args...)
			}

			// TODO: Fix Replica Logic
			// if h.executor.VerifiedReplica() && !h.replHandler.IsPartOfReplicaGroup(req.Id()) {
			// 	h.replHandler.AddToReplicaGroup(req.Id(), req)
			// }
		case key := <-keyUpdatesChan:
			// Key has been updated! check for blocked clients
			waitingArea.mu.Lock()
			for len(waitingArea.queue[key]) > 0 {
				concluded, out := h.executor.processHold(&waitingArea.queue[key][0])
				if !concluded {
					break
				}
				if len(out) > 0 {
					waitingArea.queue[key][0].req.Receive() <- &response{
						data: out,
					}
				}
				if len(waitingArea.queue[key]) > 1 {
					waitingArea.queue[key] = waitingArea.queue[key][1:]
				} else {
					delete(waitingArea.queue, key)
				}
				ls := h.executor.LStore()
				if ls.Len(key) == 0 {
					break
				}
			}
			waitingArea.mu.Unlock()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (h *Hub) Start(
	executor Executor,
	replHandler ReplicaHandler,
) {
	h.executor = executor
	h.replHandler = replHandler
	for range WORKERS_LIMIT {
		go h.StartWorker()
	}
}

func (h *Hub) Shutdown() {
	close(h.RequestChan)
	fmt.Println("Waiting for unfinished jobs")
	h.wg.Wait()
}
