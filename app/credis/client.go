package credis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"time"
)

type response struct {
	data    []byte
	isError bool
}

func (r *response) Data() []byte {
	return r.data
}

type Response interface {
	Data() []byte
}

type request struct {
	id        string
	ctx       context.Context
	cmd       Cmd
	timestamp time.Time
	args      []Token
	Client
}

func (r *request) Ctx() context.Context {
	return r.ctx
}

func (r *request) Args() []Token {
	return r.args
}

func (r *request) Cmd() Cmd {
	return r.cmd
}

type Request interface {
	Ctx() context.Context
	Args() []Token
	Cmd() Cmd
	Client
}

func NewRequest(
	client Client,
	ctx context.Context,
	cmd Cmd,
	args ...Token,
) Request {
	return &request{
		id:        GenerateString(10),
		timestamp: time.Now(),
		ctx:       ctx,
		args:      args,
		cmd:       cmd,
		Client:    client,
	}
}

type Srv interface {
	GetReplicaNums() uint
	SubscribeToReplicaUpdates(c Client) *ReplicaUpdateSubscription
}

type client struct {
	id string
	net.Conn
	Srv
	parser  *Parser
	send    chan<- Request
	receive chan Response
	tx      *TX
	exec    Executor
	sub     SubscriptionHandler
}

type Client interface {
	net.Conn
	Srv
	TryParse() (Token, int, error)
	ProcessRDB() error
	Id() string
	Send() chan<- Request
	Receive() chan Response
	WriteToMaster(cmd string, args ...Token) error
	GetTX() *TX
	GetSub() SubscriptionHandler
	Executor() Executor
}

func NewClient(conn net.Conn, srv Srv) Client {
	return &client{
		id:      GenerateString(6),
		parser:  NewParser(bufio.NewReader(conn)),
		Conn:    conn,
		Srv:     srv,
		receive: make(chan Response),
	}
}

func (c *client) WriteToMaster(cmd string, args ...Token) error {
	enc := NewEncoder()
	tokens := []Token{
		NewToken(BULK_STRING, cmd),
	}
	err := enc.Error()
	if err != nil {
		return err
	}
	tokens = append(tokens, args...)
	enc.Array(tokens...)
	enc.Commit()

	req := enc.Bytes()
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
	return c.parser.err
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

func (c *client) GetSub() SubscriptionHandler {
	return c.sub
}

func (c *client) Executor() Executor {
	return c.exec
}

func handle(client Client) {
	for {
		rawReq, _, err := client.TryParse()
		if err != nil {
			// Actual Error
			if errors.Is(err, io.EOF) {
				// Connection is closed
				break
			}
		}
		tokenType := rawReq.Type
		var resBytes []byte
		if tokenType != ARRAY {
			// Ignore that as of now
			continue
		}
		tkns := rawReq.Literal.([]Token)
		if len(tkns) == 0 {
			// Ignore that as of now
			continue
		}
		cmd, err := ParseCmd(tkns...)
		var args []Token
		if err != nil {
			enc := NewEncoder().SimpleError(err.Error()).Commit()
			client.Write(enc.Bytes())
			continue
		}
		if len(tkns) > 1 {
			args = append(args, tkns[1:]...)
		}
		ctx, cancel := context.WithCancel(context.Background())
		req := NewRequest(client, ctx, cmd, args...)
		sendAndCancel := func(data []byte) {
			client.Write(data)
			cancel()
		}
		if !client.GetSub().IsAllowed(cmd) {
			sendAndCancel(NewEncoder().SimpleError((&NoOtherCommandsInSubscribeContext{cmd: cmd.String()}).Error()).Commit().Bytes())
			continue
		}
		if cmd.String() == EXEC {
			sendAndCancel(req.GetTX().Exec(req))
			continue
		}
		send := client.Send()
		if client.GetTX().IsMulti() && !slices.Contains([]string{MULTI, DISCARD}, cmd.String()) {
			sendAndCancel(client.GetTX().Enqueue(req))
		} else if spec, ok := cmd.(*BLPOPSpecs); ok && spec.Lifetime != nil {
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
				resBytes = NewEncoder().NullArray()
			case res := <-client.Receive():
				resBytes = res.Data()
			}
			timer.Stop()
			sendAndCancel(resBytes)
		} else if spec, ok := cmd.(*WAITSpecs); ok {
			timeout := spec.Timeout
			currentReplicas := client.GetReplicaNums()
			if currentReplicas >= uint(spec.NumReplicas) {
				resBytes = NewEncoder().Integer(int(currentReplicas)).Commit().Bytes()
			} else {
				timer := time.NewTicker(time.Duration(timeout))
				sub := client.SubscribeToReplicaUpdates(client)
				wait := true
				for wait {
					select {
					case <-timer.C:
						// Expired
						wait = false
					case updated := <-sub.C:
						if updated >= uint(spec.NumReplicas) {
							currentReplicas = updated
							wait = false
						}
					}
				}
				timer.Stop()
				resBytes = NewEncoder().Integer(int(currentReplicas)).Commit().Bytes()
			}
			sendAndCancel(resBytes)
		} else {
			client.Send() <- req
			res := <-client.Receive()
			sendAndCancel(res.Data())
		}
	}
}
