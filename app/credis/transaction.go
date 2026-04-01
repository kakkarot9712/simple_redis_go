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

func (tx *TX) Discard() []byte {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	enc := NewEncoder()
	if !tx.multi {
		enc.SimpleError((&ErrDiscardWithoutMulti{}).Error()).Commit()
	} else {
		tx.txs = LinkedList[Request]{}
		tx.multi = false
		enc.Ok()
	}
	return enc.Bytes()
}

func (tx *TX) Exec(client Client, ctx context.Context) []byte {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	enc := NewEncoder()
	responses := [][]byte{}
	if !tx.multi {
		enc.SimpleError((&ErrExecWithoutMulti{}).Error()).Commit()
	} else {
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
	}
	enc.ArrayRaw(responses).Commit()
	tx.multi = false
	return enc.Bytes()
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
	return NewEncoder().SimpleString("QUEUED").Commit().Bytes()
}

func (tx *TX) IsMulti() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.multi
}
