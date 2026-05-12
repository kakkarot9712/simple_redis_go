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
	Start(executor Executor)
	StartWorker()
	RequestChannel() chan Request
	Executor() Executor
}

type hub struct {
	requestChan chan Request
	wg          sync.WaitGroup
	executor    Executor
	watcher     Watcher
	replHandler ReplicaManager
	store       ListStore[string]
}

func NewHub(
	str ListStore[string],
	watcher Watcher,
	replManager ReplicaManager,
) Hub {
	handler := make(chan Request, WORKERS_LIMIT)
	return &hub{
		requestChan: handler,
		watcher:     watcher,
		store:       str,
		replHandler: replManager,
	}
}

func (h *hub) StartWorker() {
	send := h.watcher.Send()
	go h.watcher.Start()
	for {
		select {
		case req, ok := <-h.requestChan:
			if !ok {
				h.wg.Done()
				return
			}
			res := h.executor.Exec(req)
			spec := req.Specs()
			if spec, ok := spec.(*BLPOPSpecs); ok && !spec.Concluded {
				continue
			}
			req.Receive() <- res
			cmd := req.Specs().String()

			// Propagate to replicas
			args := req.Args()

			switch cmd {
			case SET:
				spec := req.Specs().(*SETSpecs)
				send <- spec.Key
			case INCR:
				spec := req.Specs().(*INCRSpecs)
				send <- spec.Key
			}

			if res.DoNotPropagate() {
				continue
			}

			if Writeable(cmd) {
				stream := []Token{
					NewToken(BULK_STRING, cmd),
				}
				stream = append(stream, args...)
				h.replHandler.Propagator() <- NewEncoder().Array(stream...)
			}

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
				ls := h.store
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

func (h *hub) Start(executor Executor) {
	h.executor = executor
	for range WORKERS_LIMIT {
		h.wg.Add(1)
		go h.StartWorker()
	}
}

func (h *hub) Shutdown() {
	close(h.requestChan)
	fmt.Println("Waiting for unfinished jobs")
	h.wg.Wait()
	h.watcher.Stop()
}

func (h *hub) RequestChannel() chan Request {
	return h.requestChan
}

func (h *hub) Executor() Executor {
	return h.executor
}

func (h *hub) Watcher() Watcher {
	return h.watcher
}
