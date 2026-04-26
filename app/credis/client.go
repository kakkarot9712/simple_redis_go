package credis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type response struct {
	data      []byte
	artifacts any // This will contain other data depending on command
	isError   bool
}

func (r *response) Data() []byte {
	return r.data
}

func (r *response) Artifacts() any {
	return r.artifacts
}

type Response interface {
	Data() []byte
	Artifacts() any
}

type request struct {
	id        string
	ctx       context.Context
	specs     Specs
	timestamp time.Time
	args      []Token
	client    Client
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

func (r *request) Client() Client {
	return r.client
}

func (r *request) SetSpecs(specs Specs) {
	r.specs = specs
}

func (r *request) SetArgs(args ...Token) {
	r.args = args
}

type Spec interface {
	Execute(e *executor, req Request) Response
}

type Request interface {
	Ctx() context.Context
	Args() []Token
	Specs() Specs
	Client() Client
	SetSpecs(Specs)
	SetArgs(args ...Token)
}

func NewRequest(
	client Client,
	ctx context.Context,
) Request {
	return &request{
		id:        GenerateString(10),
		timestamp: time.Now(),
		ctx:       ctx,
		client:    client,
	}
}

type client struct {
	mu sync.RWMutex
	id string
	net.Conn
	srv              Server
	parser           Parser
	send             chan<- Request
	receive          chan Response
	tx               *TX
	subCancelMapping map[string]func() // cancel func mapping per channel
	exec             Executor
	processed        *atomic.Uint64
	auth             map[string]Auth
	currentUser      string
	isAuthenticated  bool
	watchList        map[string]struct {
		Watching bool
		Dirty    bool
	}
	sortedSet SortedSet
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
	Srv() Server
	CancelSub(channel string)
	AddSub(channelId string, cancel func())
	ProcessedAtomic() *atomic.Uint64
	CurrentUser() string
	IsAuthenticated() bool
	Authenticate(user string, password string) bool
	SortedSet() SortedSet
	Watch(cmd string) *CmdNotifier
	IsWatching(cmd string) bool
	MakeDirty(cmd string)
	IsDirty() bool
	TerminateWatcher()
}

func NewClient(conn net.Conn, srv Server) Client {
	return &client{
		id:               GenerateString(6),
		parser:           NewParser(bufio.NewReader(conn)),
		Conn:             conn,
		srv:              srv,
		tx:               NewTX(),
		send:             srv.Hub().RequestChannel(),
		receive:          make(chan Response),
		subCancelMapping: make(map[string]func()),
		exec:             srv.Hub().Executor(),
		currentUser:      DefaultAuth().user,
		isAuthenticated:  !srv.Auth(DefaultAuth().user).PassRequired(),
		sortedSet:        NewSortedSet(),
		watchList: make(map[string]struct {
			Watching bool
			Dirty    bool
		}),
	}
}

func (c *client) Srv() Server {
	return c.srv
}

func (c *client) SortedSet() SortedSet {
	return c.sortedSet
}

func (c *client) IsAuthenticated() bool {
	return c.isAuthenticated
}

func (c *client) CancelSub(channelId string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subCancelMapping[channelId] != nil {
		c.subCancelMapping[channelId]()
		delete(c.subCancelMapping, channelId)
	}
}

func (c *client) Watch(cmd string) *CmdNotifier {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watchList[cmd] = struct {
		Watching bool
		Dirty    bool
	}{true, false}
	fmt.Printf("Client %v: Key %v is in watchlist\n", c.id, cmd)
	return c.srv.Hub().Watcher().Add(c.id)
}

func (c *client) IsWatching(cmd string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.watchList[cmd].Watching
}

func (c *client) MakeDirty(cmd string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.watchList[cmd].Watching {
		d := c.watchList[cmd]
		d.Dirty = true
		c.watchList[cmd] = d
	}
}

func (c *client) IsDirty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, v := range c.watchList {
		if v.Dirty {
			return true
		}
	}
	return false
}

func (c *client) TerminateWatcher() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.srv.Hub().Watcher().Cancel(c.id)
	c.watchList = make(map[string]struct {
		Watching bool
		Dirty    bool
	})
}

func (c *client) AddSub(channelId string, cancel func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subCancelMapping[channelId] == nil {
		c.subCancelMapping[channelId] = func() {}
	}
	c.subCancelMapping[channelId] = cancel
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

func (c *client) ProcessedAtomic() *atomic.Uint64 {
	return c.processed
}

func (c *client) CurrentUser() string {
	return c.currentUser
}

func (c *client) Authenticate(user string, password string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.Srv().Auth(user).Authenticate(password) {
		return false
	}
	c.currentUser = user
	c.isAuthenticated = true
	return true
}

func handle(client Client) {
	clientCtx, clientCancel := context.WithCancel(context.Background())
	cmdNotifier := false
	isAuthenticated := client.IsAuthenticated()
	user := client.CurrentUser()
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

		if tokenType != ARRAY {
			// Ignore that as of now
			continue
		}
		tkns := rawReq.Literal.([]Token)
		if len(tkns) == 0 {
			continue
		}
		var artifacts any
		reqCtx, cancel := context.WithCancel(clientCtx)
		sendAndCancel := func(res Response) {
			client.Write(res.Data())
			artifacts = res.Artifacts()
			cancel()
		}

		buffLen := uint(math.Min(float64(2), float64(len(tkns))))
		argsIndex, cmd, err := ParseCmd(tkns[:buffLen]...)
		if !isAuthenticated && cmd != AUTH {
			sendAndCancel(&response{
				data: NewEncoder().SimpleError("NOAUTH Authentication required."),
			})
			continue
		}
		var args []Token
		if len(tkns) > argsIndex {
			args = tkns[argsIndex:]
		}
		if cmd == AUTH {
			s, err := ParseSpec(cmd, args...)
			authSpec := s.(*AUTHSpecs)
			if err != nil {
				client.Write(NewEncoder().SimpleError(err.Error()))
				continue
			}
			if client.Authenticate(authSpec.Username, authSpec.Password) {
				isAuthenticated = true
				user = authSpec.Username
				client.Write(NewEncoder().Ok())
			} else {
				client.Write(NewEncoder().SimpleError((&ErrAuthWrongPassword{}).Error()))
			}
			continue
		} else if cmd == EXEC {
			sendAndCancel(&response{
				data: client.GetTX().Exec(client, reqCtx),
			})
			continue
		} else if cmd == ACL_WHOAMI {
			sendAndCancel(&response{
				data: NewEncoder().BulkString(&user),
			})
			continue
		} else if cmd == WATCH && client.GetTX().IsMulti() {
			sendAndCancel(&response{
				data: NewEncoder().SimpleError("ERR WATCH inside MULTI is not allowed"),
			})
			continue
		}

		req := NewRequest(client, reqCtx)
		specs, err := ParseSpec(cmd, args...)
		req.SetSpecs(specs)
		if err != nil {
			client.Write(NewEncoder().SimpleError(err.Error()))
			continue
		}
		if len(tkns) > 1 {
			args = append(args, tkns[1:]...)
		}

		if !client.Srv().SubManager().IsAllowed(cmd, client.Id()) {
			sendAndCancel(&response{
				data: NewEncoder().SimpleError((&NoOtherCommandsInSubscribeContext{cmd: cmd}).Error()),
			})
			continue
		}

		send := client.Send()
		if client.GetTX().IsMulti() && !slices.Contains([]string{MULTI, DISCARD}, cmd) {
			sendAndCancel(&response{
				data: client.GetTX().Enqueue(req),
			})
		} else if spec, ok := specs.(*BLPOPSpecs); ok && spec.Lifetime != nil {
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
		} else if spec, ok := specs.(*WAITSpecs); ok {
			timeout := spec.Timeout
			var res Response
			currentReplicas := client.Srv().GetReplicaNums()
			if currentReplicas >= uint(spec.NumReplicas) {
				res = &response{
					data: NewEncoder().Integer(int(currentReplicas)),
				}
			} else {
				timer := time.NewTicker(time.Duration(timeout))
				sub := client.Srv().SubscribeToReplicaUpdates(client)
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
				res = &response{
					data: NewEncoder().Integer(int(currentReplicas)),
				}
			}
			sendAndCancel(res)
		} else {
			client.Send() <- req
			res := <-client.Receive()
			sendAndCancel(res)
		}

		// Do other tasks below using artifacts, response has been sent from below
		if artifacts != nil {
			switch cmd {
			case SUBSCRIBE:
				if sub, ok := artifacts.(*Sub); ok {
					client.AddSub(sub.Channel, sub.Cancel)
					go ListenForMsgs(clientCtx, sub, client)
				}
			case WATCH:
				if notifier, ok := artifacts.(*CmdNotifier); ok && !cmdNotifier {
					// TODO: Maybe convert to atomic???
					cmdNotifier = true
					go func() {
						for key := range notifier.C {
							client.MakeDirty(key)
						}
						cmdNotifier = false
					}()
				}
			}
		}
	}
	clientCancel()
	client.TerminateWatcher()
}
