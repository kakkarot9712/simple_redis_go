package credis

import (
	"fmt"
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

type Hub interface {
	Shutdown()
	Start(
		executor Executor,
		replHandler Server,
	)
	StartWorker()
	RequestChannel() chan Request
	Executor() Executor
}

type hub struct {
	requestChan chan Request
	wg          sync.WaitGroup
	executor    Executor
	replHandler Server
}

func NewHub() Hub {
	handler := make(chan Request, WORKERS_LIMIT)
	return &hub{
		requestChan: handler,
	}
}

func (h *hub) StartWorker() {
	h.wg.Add(1)
	for {
		select {
		case req, ok := <-h.requestChan:
			if !ok {
				h.wg.Done()
				break
			}
			res := h.executor.Exec(req)
			spec := req.Specs()
			if spec, ok := spec.(*BLPOPSpecs); ok && !spec.Concluded {
				continue
			}
			req.Client().Receive() <- res
			// Propagate to replicas
			cmd := req.Specs().String()
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
					waitingArea.queue[key][0].req.Client().Receive() <- &response{
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

func (h *hub) Start(
	executor Executor,
	replHandler Server,
) {
	h.executor = executor
	h.replHandler = replHandler
	for range WORKERS_LIMIT {
		go h.StartWorker()
	}
}

func (h *hub) Shutdown() {
	close(h.requestChan)
	fmt.Println("Waiting for unfinished jobs")
	h.wg.Wait()
}

func (h *hub) RequestChannel() chan Request {
	return h.requestChan
}

func (h *hub) Executor() Executor {
	return h.executor
}
