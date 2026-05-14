package credis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type response struct {
	data           []byte
	artifacts      any // This will contain other data depending on command
	isError        bool
	doNotPropagate bool
}

func (r *response) Data() []byte {
	return r.data
}

func (r *response) Artifacts() any {
	return r.artifacts
}

func (r *response) DoNotPropagate() bool {
	return r.doNotPropagate
}

func (r *response) Set(data []byte, artifacts any, doNotPropagate bool) {
	r.data = data
	r.artifacts = artifacts
	r.doNotPropagate = doNotPropagate
}

type Response interface {
	Set(data []byte, artifacts any, doNotPropagate bool)
	Data() []byte
	DoNotPropagate() bool
	Artifacts() any
}

type request struct {
	id        string
	client    Client
	authCtx   *authContext
	txs       *TX
	ctx       context.Context
	specs     Specs
	timestamp time.Time
	args      []Token
	receiver  chan<- Response
}

func (r *request) Ctx() context.Context {
	return r.ctx
}

func (r *request) Args() []Token {
	return r.args
}

func (r *request) Specs() Specs {
	return r.specs
}

func (r *request) SetSpecs(specs Specs) {
	r.specs = specs
}

func (r *request) SetArgs(args ...Token) {
	r.args = args
}

func (r *request) Receive() chan<- Response {
	return r.receiver
}

func (r *request) ClientId() string {
	return r.client.Id()
}

func (r *request) Client() Client {
	return r.client
}

func (r *request) AuthCtx() *authContext {
	return r.authCtx
}

func (r *request) TX() *TX {
	return r.txs
}

type Spec interface {
	Execute(e *executor, req Request) Response
}

type Request interface {
	Ctx() context.Context
	ClientId() string
	Client() Client
	Args() []Token
	Specs() Specs
	SetSpecs(Specs)
	SetArgs(args ...Token)
	Receive() chan<- Response
	AuthCtx() *authContext
	TX() *TX
}

func NewRequest(
	ctx context.Context,
	authCtx *authContext,
	txs *TX,
	client Client,
	receiver chan<- Response,
) Request {
	return &request{
		id:        GenerateString(10),
		timestamp: time.Now(),
		authCtx:   authCtx,
		txs:       txs,
		ctx:       ctx,
		client:    client,
		receiver:  receiver,
	}
}

type client struct {
	mu sync.RWMutex
	id string
	net.Conn
	ctx       context.Context
	cancel    context.CancelFunc
	parser    Parser
	authCtx   *authContext
	send      chan<- Request
	receive   chan Response
	tx        *TX
	exec      Executor
	processed *atomic.Uint64
}

type Client interface {
	net.Conn
	TryParse() (Token, int, error)
	ProcessRDB() error
	Id() string
	Send() chan<- Request
	Receive() chan Response
	WriteToMaster(cmd string, args ...Token) error
	GetTX() *TX
	Executor() Executor
	AuthCtx() *authContext
	Ctx() context.Context
	Cancel() context.CancelFunc
}

func NewClient(
	conn net.Conn,
	send chan<- Request,
	user string,
	isAuthenticated bool,
) Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &client{
		ctx:     ctx,
		cancel:  cancel,
		id:      GenerateString(6),
		parser:  NewParser(bufio.NewReader(conn)),
		Conn:    conn,
		tx:      NewTX(),
		send:    send,
		receive: make(chan Response),
		// subCancelMapping: make(map[string]func()),
		authCtx: NewAuthContext(),
	}
}

func (c *client) AuthCtx() *authContext {
	return c.authCtx
}

func (c *client) WriteToMaster(cmd string, args ...Token) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	enc := NewEncoder()
	tokens := []Token{
		NewToken(BULK_STRING, cmd),
	}
	err := enc.Error()
	if err != nil {
		return err
	}
	tokens = append(tokens, args...)
	req := enc.Array(tokens...)
	c.Conn.Write(req)
	return nil
}

func (c *client) TryParse() (Token, int, error) {
	token, len := c.parser.TryParse()
	err := c.parser.Error()
	if err != nil {
		return NewToken(ARRAY, []any{}), 0, err
	}
	return token, len, err
}

func (c *client) ProcessRDB() error {
	c.parser.ProcessRDB()
	return c.parser.Error()
}

func (c *client) Id() string {
	return c.id
}

func (c *client) Send() chan<- Request {
	return c.send
}

func (c *client) Receive() chan Response {
	return c.receive
}

func (c *client) GetTX() *TX {
	return c.tx
}

func (c *client) Executor() Executor {
	return c.exec
}

func (c *client) Ctx() context.Context {
	return c.ctx
}

func (c *client) Cancel() context.CancelFunc {
	return c.cancel
}

func handle(client Client) {
l:
	for {
		rawReq, _, err := client.TryParse()
		if err != nil {
			// Actual Error
			if errors.Is(err, io.EOF) {
				// Connection is closed
			} else {
				fmt.Printf("Client %v: Something went wrong! :: %v\n", client.Id(), err.Error())
			}
			break l
		}
		tokenType := rawReq.Type

		if tokenType != ARRAY {
			// Ignore that as of now
			continue
		}
		var tkns []Token
		if t, ok := rawReq.Literal.([]Token); !ok {
			continue
		} else {
			tkns = t
		}
		if len(tkns) == 0 {
			continue
		}
		// var artifacts any
		reqCtx, cancel := context.WithCancel(client.Ctx())
		sendAndCancel := func(res Response) {
			client.Write(res.Data())
			// artifacts = res.Artifacts()
			cancel()
		}

		buffLen := uint(math.Min(float64(2), float64(len(tkns))))
		argsIndex, cmd, err := ParseCmd(tkns[:buffLen]...)

		var args []Token
		if len(tkns) > argsIndex {
			args = tkns[argsIndex:]
		}
		req := NewRequest(reqCtx, client.AuthCtx(), client.GetTX(), client, client.Receive())
		specs, err := ParseSpec(cmd, args...)
		req.SetSpecs(specs)
		req.SetArgs(args...)
		if err != nil {
			client.Write(NewEncoder().SimpleError(err.Error()))
			continue
		}

		send := client.Send()
		if spec, ok := specs.(*BLPOPSpecs); ok && spec.Lifetime != nil {
			var res Response
			deadline := time.Duration(*spec.Lifetime * float64(time.Second))
			timer := time.NewTimer(deadline)
			go func() {
				select {
				case send <- req:
				case <-req.Ctx().Done():
				}
			}()
			select {
			case <-timer.C:
				res = &response{
					data: NewEncoder().NullArray(),
				}
			case res = <-client.Receive():
			}
			timer.Stop()
			sendAndCancel(res)
		} else {
			client.Send() <- req
			res := <-client.Receive()
			sendAndCancel(res)
		}

		// Do other tasks below using artifacts, response has been sent already starting from below
		// if artifacts != nil {
		// 	switch typedArtifact := artifacts.(type) {
		// 	case Repl:
		// 		<-typedArtifact.C
		// 		break l
		// 	}
		// }
	}
	client.Cancel()
	client.Close()
	// client.TerminateWatcher()
}
