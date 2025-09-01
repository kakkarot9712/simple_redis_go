package credis

import (
	"fmt"
	"net"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/credis/executor"
	"github.com/codecrafters-io/redis-starter-go/app/credis/helpers"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
)

const WORKERS_LIMIT = 6

type Hub struct {
	Send    chan<- Conn
	receive <-chan Conn
	wg      sync.WaitGroup
}

type ParserProvider interface {
	TryParse() (tokens.Token, int)
	Error() error
}

type Encoder interface {
	Error() error
	Bytes() []byte
	Array(args []tokens.Token)
	BulkString(data string)
	SimpleString(data string)
	Integer(data int)
	SimpleError(err string)
	ArrayRaw(rawData [][]byte)
}

type ExecutorProvider interface {
	Error() error
}

type ReplicaHandler interface {
	AddToReplicaGroup(conn Conn)
	PropagateToReplicaGroup(tokens ...tokens.Token)
	IsPartOfReplicaGroup(id string) bool
	RemoveFromReplicaLGroup(id string)
}

func NewHub() *Hub {
	handler := make(chan Conn, WORKERS_LIMIT)
	return &Hub{
		Send:    handler,
		receive: handler,
	}
}

func (h *Hub) Start() {
	for range WORKERS_LIMIT {
		h.wg.Add(1)
		go func() {
			for c := range h.receive {
				handle(c)
			}
			// for {
			// 	select {
			// 	case c, ok := <-h.receive:
			// 		if !ok {
			// 			break loop
			// 		}
			// 		handle(c.Conn)
			// 	default:
			// 		// Worker Idle
			// 		fmt.Println("Worker Idle")
			// 	}
			// }
			h.wg.Done()
		}()
	}
}

func (h *Hub) Shutdown() {
	fmt.Println("Waiting for unfinished jobs")
	h.wg.Wait()
}

func (h *Hub) SendConn(
	conn net.Conn,
	parser ParserProvider,
	exec executor.Executor,
	replHandler ReplicaHandler,
) {
	h.Send <- Conn{
		Id:          helpers.GenerateString(10),
		Conn:        conn,
		Parser:      parser,
		Executor:    exec,
		replHandler: replHandler,
	}
}
