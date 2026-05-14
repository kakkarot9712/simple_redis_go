package credis

import (
	"context"
	"slices"
	"sync"
)

type TX struct {
	mu    sync.RWMutex
	txs   LinkedList[Request]
	multi bool
}

func NewTX() *TX {
	return &TX{}
}

func (tx *TX) Discard() []byte {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	var data []byte
	if !tx.multi {
		data = NewEncoder().SimpleError((&ErrDiscardWithoutMulti{}).Error())
	} else {
		tx.txs = LinkedList[Request]{}
		tx.multi = false
		data = NewEncoder().Ok()
	}
	return data
}

// TODO: Improve logic
func (tx *TX) Exec(
	ctx context.Context,
	exec *executor,
	clientId string,
) []byte {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	var data []byte
	enc := NewEncoder()
	responses := [][]byte{}
	if !tx.multi {
		data = enc.SimpleError((&ErrExecWithoutMulti{}).Error())
	} else {
		if exec.deps.Watcher.IsModified(clientId) {
			tx.txs = LinkedList[Request]{}
			tx.multi = false
			exec.deps.Watcher.Cancel(clientId)
			return NewEncoder().NullArray()
		}
		cleanExec := NewExec(exec.deps)
		cleanExec.Use(ExecutorMiddleware)
		for {
			r := tx.txs.Remove(0)
			if r == nil {
				break
			}
			relayReq := NewRequest(
				context.Background(),
				DefaultAuthContext(),
				nil,
				(*r).Client(),
				nil,
			)
			relayReq.SetSpecs((*r).Specs())
			out := cleanExec.Exec(relayReq).Data()
			responses = append(responses, out)
		}
		data = enc.ArrayRaw(responses)
	}
	exec.deps.Watcher.Cancel(clientId)
	tx.multi = false
	return data
}

func (tx *TX) Multi() []byte {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.multi = true
	return NewEncoder().Ok()
}

func (tx *TX) Enqueue(req Request) []byte {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.txs.Append(req)
	return NewEncoder().SimpleString("QUEUED")
}

func (tx *TX) IsMulti() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.multi
}

func TransactionMiddleware(e *executor, req Request, res Response, terminate TerminateFunc) {
	if req.TX().IsMulti() && !slices.Contains([]string{MULTI, DISCARD, EXEC}, req.Specs().String()) {
		res.Set(req.TX().Enqueue(req), nil, false)
		terminate()
		return
	}
}
