package credis

import (
	"context"
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

func (tx *TX) Discard(client Client) []byte {
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
	client.TerminateWatcher()
	return data
}

// TODO: Improve logic
func (tx *TX) Exec(client Client, ctx context.Context) []byte {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	var data []byte
	enc := NewEncoder()
	responses := [][]byte{}
	if !tx.multi {
		data = enc.SimpleError((&ErrExecWithoutMulti{}).Error())
	} else {
		for _, c := range tx.txs.Get(0, int64(tx.txs.length)) {
			var key string
			switch c.Specs().String() {
			case SET:
				spec := c.Specs().(*SETSpecs)
				key = spec.Key
			case INCR:
				spec := c.Specs().(*INCRSpecs)
				key = spec.Key
			}
			if key != "" && client.IsDirty() {
				tx.txs = LinkedList[Request]{}
				tx.multi = false
				client.TerminateWatcher()
				return enc.NullArray()
			}
		}
		for {
			r := tx.txs.Remove(0)
			if r == nil {
				break
			}
			executor := client.Executor()
			relayReq := NewRequest(
				client,
				(*r).Ctx(),
			)
			relayReq.SetSpecs((*r).Specs())
			out := executor.Exec(relayReq).Data()
			responses = append(responses, out)
		}
		data = enc.ArrayRaw(responses)
	}
	tx.multi = false
	client.TerminateWatcher()
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
