package credis

import (
	"sync"
	"sync/atomic"
	"time"
)

type BLPOPHold struct {
	req     Request
	resp    []string
	keys    []string
	timeout *time.Time
}

type InfoProvider interface {
	Get(section string, key string) string
	Section(section string) map[string]string
}

type RDBConfigProvider interface {
	GetRDBFileName() string
	GetRDBDir() string
}

type Executor interface {
	Error() error
	Exec(req Request, opts ...ExecOption) []byte
	processHold(hold *BLPOPHold) (concluded bool, resData []byte)
	LStore() ListStore[string]
}

type executor struct {
	mu    sync.RWMutex
	err   error
	store struct {
		KV     KVStore
		Stream Stream
		List   ListStore[string]
	}
	serverInfo InfoProvider
	rdbConfig  RDBConfigProvider
	processed  *atomic.Uint64
}

type ExecOpts struct {
	processedBytes *atomic.Uint64
}

type ExecOption func(*ExecOpts)

func WithProcessedBytesAtomic(
	processedBytes *atomic.Uint64,
) ExecOption {
	return func(cfg *ExecOpts) {
		cfg.processedBytes = processedBytes
	}
}

func NewExec(
	str struct {
		KV     KVStore
		Stream Stream
		List   ListStore[string]
	},
	srvinfo InfoProvider,
	rdbConfig RDBConfigProvider,
) Executor {
	return &executor{
		store:      str,
		serverInfo: srvinfo,
		rdbConfig:  rdbConfig,
	}
}

func (e *executor) Error() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.err
}

func (e *executor) LStore() ListStore[string] {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.store.List
}

func (e *executor) Exec(req Request, opts ...ExecOption) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	cfg := ExecOpts{}
	for _, opt := range opts {
		opt(&cfg)
	}
	e.processed = cfg.processedBytes
	return req.Cmd().Execute(e, req)
}

func (e *executor) parseError(err error, enc *Encoder) (bool, []byte) {
	if err == nil {
		return false, nil
	}
	enc.SimpleError(err.Error())
	enc.Commit()
	return true, enc.Bytes()
}

func (e *executor) processHold(hold *BLPOPHold) (concluded bool, resData []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	req := hold.req
	select {
	case <-req.Ctx().Done():
		concluded = true
	default:
		for i := 0; i < len(hold.keys); i++ {
			key := hold.keys[i]
			popped := e.store.List.Pop(key)
			if popped == nil {
				return
			}
			hold.resp = append(hold.resp, key, *popped)
		}
		concluded = true
		tokens := []Token{}
		for _, p := range hold.resp {
			tokens = append(tokens, NewToken(BULK_STRING, p))
		}
		resData = NewEncoder().Array(tokens...).Commit().Bytes()
	}
	return
}
