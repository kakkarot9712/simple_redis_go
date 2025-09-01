package executor

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/credis/commands"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
)

type TX struct {
	cmd  string
	args []tokens.Token
}

func multiExcludedCommands() []string {
	return []string{
		commands.EXEC,
		commands.DISCARD,
	}
}

type TXQueue struct {
	mu  sync.RWMutex
	txs []TX
}

func NewTXQueue() *TXQueue {
	return &TXQueue{}
}

func NewTX(cmd string, args []tokens.Token) TX {
	return TX{
		cmd:  cmd,
		args: args,
	}
}

func (e *executor) multi() {
	enc := encoder.New()
	e.multiMode = true
	enc.SimpleString("OK")
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

func (e *executor) execMulti() {
	enc := encoder.New()
	if !e.multiMode {
		e.handleError(&ErrExecWithoutMulti{}, enc)
		return
	}
	e.multiMode = false
	responses := [][]byte{}
	e.txQueue.mu.Lock()
	defer func() {
		e.txQueue.txs = make([]TX, 0)
		e.txQueue.mu.Unlock()
	}()
	for _, tx := range e.txQueue.txs {
		e.Exec(tx.cmd, tx.args)
		responses = append(responses, e.out)
	}
	enc.ArrayRaw(responses)
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

func (e *executor) appendTx(cmd string, args []tokens.Token) {
	enc := encoder.New()
	e.txQueue.mu.Lock()
	defer e.txQueue.mu.Unlock()

	e.txQueue.txs = append(e.txQueue.txs, NewTX(cmd, args))
	enc.SimpleString("QUEUED")
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

func (e *executor) discard() {
	enc := encoder.New()
	if !e.multiMode {
		e.handleError(&ErrDiscardWithoutMulti{}, enc)
		return
	}
	e.txQueue.mu.Lock()
	defer e.txQueue.mu.Unlock()
	e.txQueue.txs = make([]TX, 0)
	e.multiMode = false
	enc.SimpleString("OK")
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}
