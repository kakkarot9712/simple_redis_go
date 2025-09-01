package credis

import (
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/credis/executor"
	"github.com/codecrafters-io/redis-starter-go/app/credis/store"
)

type Conn struct {
	Conn        net.Conn
	Executor    executor.Executor
	Parser      ParserProvider
	Encoder     Encoder
	Id          string
	Store       store.BaseStore
	replHandler ReplicaHandler
}
