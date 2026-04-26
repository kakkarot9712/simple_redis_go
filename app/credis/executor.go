package credis

import (
	"time"
)

type BLPOPHold struct {
	req     Request
	resp    []string
	keys    []string
	timeout *time.Time
}

type RDBConfigProvider interface {
	GetRDBFileName() string
	GetRDBDir() string
}

type Executor interface {
	Exec(req Request) Response
	processHold(hold *BLPOPHold) (concluded bool, resData []byte)
	LStore() ListStore[string]
}

type Exec interface {
	Execute(e *executor, req Request) Response
}

// Executor must remain stateless to allow concurrent usage
type executor struct {
	store struct {
		KV     KVStore
		Stream Stream
		List   ListStore[string]
	}
	// TODO: Need mutex for serverInfo?
	serverInfo ServerInfo
	rdbConfig  RDBConfigProvider
}

func NewExec(
	str dataStores,
	srvinfo ServerInfo,
	rdbConfig RDBConfigProvider,
) Executor {
	return &executor{
		store:      str,
		serverInfo: srvinfo,
		rdbConfig:  rdbConfig,
	}
}

func (e *executor) LStore() ListStore[string] {
	return e.store.List
}

func (e *executor) Exec(req Request) Response {
	// e.processed = cfg.processedBytes
	if vp, ok := req.Specs().(Exec); ok {
		return vp.Execute(e, req)
	} else {
		return notImplemented(req.Specs().String())
	}
}

func (e *executor) processHold(hold *BLPOPHold) (concluded bool, resData []byte) {
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
		resData = NewEncoder().Array(tokens...)
	}
	return
}
