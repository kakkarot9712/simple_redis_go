package credis

import (
	"sync"
)

type Watcher interface {
	Add(clientId string) *CmdNotifier
	Start()
	Stop()
	Send() chan<- string
	Cancel(clientId string)
}

type CmdNotifier struct {
	C <-chan string
}

type cmdWatcher struct {
	mu   sync.RWMutex
	recv chan string

	// Clients will be added here
	listeners map[string]chan string
}

func NewWatcher() Watcher {
	c := make(chan string)
	return &cmdWatcher{
		recv:      c,
		listeners: make(map[string]chan string),
	}
}

func (w *cmdWatcher) Send() chan<- string {
	return w.recv
}

func (w *cmdWatcher) Start() {
	for cmd := range w.recv {
		go func() {
			w.mu.RLock()
			listeners := w.listeners
			w.mu.RUnlock()
			for _, l := range listeners {
				l <- cmd
			}
		}()
	}
}

func (w *cmdWatcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	close(w.recv)
}

func (w *cmdWatcher) Add(clientId string) *CmdNotifier {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.listeners[clientId]
	if c == nil {
		c = make(chan string)
		w.listeners[clientId] = c
	}
	notifier := &CmdNotifier{
		C: c,
	}
	return notifier
}

func (w *cmdWatcher) Cancel(clientId string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.listeners[clientId]
	if c == nil {
		return
	}
	delete(w.listeners, clientId)
	close(c)
}
