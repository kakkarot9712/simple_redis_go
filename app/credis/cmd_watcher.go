package credis

import (
	"sync"
)

type Watcher interface {
	Add(clientId string, key ...string)
	Start()
	Stop()
	Send() chan<- string
	Cancel(clientId string)
	IsModified(clientId string) bool
}

type cmdWatcher struct {
	mu   sync.RWMutex
	recv chan string

	// Key => clientId => watching
	watchList map[string]map[string]bool

	// clientId => dirty
	modifiedClients map[string]bool

	// ClientId => unsub
	cancelList map[string]bool
}

func NewWatcher() Watcher {
	c := make(chan string)
	return &cmdWatcher{
		recv:            c,
		watchList:       make(map[string]map[string]bool),
		cancelList:      make(map[string]bool),
		modifiedClients: make(map[string]bool),
	}
}

func (w *cmdWatcher) Send() chan<- string {
	return w.recv
}

func (w *cmdWatcher) Start() {
	for cmd := range w.recv {
		go func() {
			w.mu.Lock()
			watchers := w.watchList[cmd]
			for cid := range watchers {
				if w.cancelList[cid] {
					delete(watchers, cid)
				} else {
					w.modifiedClients[cid] = true
				}
			}
			w.watchList[cmd] = watchers
			w.mu.Unlock()
		}()
	}
}

func (w *cmdWatcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	close(w.recv)
}

func (w *cmdWatcher) Add(clientId string, keys ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, k := range keys {
		c := w.watchList[k]
		if c == nil {
			c = make(map[string]bool)
			w.watchList[k] = c
		}
		w.watchList[k][clientId] = true
		w.modifiedClients[clientId] = false
	}
}

func (w *cmdWatcher) Cancel(clientId string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cancelList[clientId] = true
	delete(w.modifiedClients, clientId)
}

func (w *cmdWatcher) IsModified(clientId string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.modifiedClients[clientId]
}

func WatchCommandGuardMiddleware(e *executor, req Request, res Response, terminate TerminateFunc) {
	if req.Specs().String() == WATCH && req.TX().IsMulti() {
		terminate(&ErrWatchInsideMulti{})
		return
	}
}
