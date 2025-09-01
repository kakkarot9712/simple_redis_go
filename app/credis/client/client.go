package client

import (
	"bufio"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/parser"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"
)

type redisClient struct {
	conn   net.Conn
	err    error
	parser *parser.Parser
}

type RedisClient interface {
	Send(cmd string, args ...tokens.Token)
	TryParse() (tokens.Token, int)
	ProcessRDB()
	Error() error
}

func New(conn net.Conn) RedisClient {
	return &redisClient{
		parser: parser.New(bufio.NewReader(conn)),
		conn:   conn,
	}
}

func (c *redisClient) Error() error {
	return c.err
}

func (c *redisClient) Send(cmd string, args ...tokens.Token) {
	enc := encoder.New()
	tokens := []tokens.Token{
		tokens.New(types.BULK_STRING, cmd),
	}
	c.err = enc.Error()
	if c.err != nil {
		return
	}
	tokens = append(tokens, args...)
	enc.Array(tokens...)
	enc.Commit()

	req := enc.Bytes()
	c.conn.Write(req)
}

func (c *redisClient) TryParse() (tokens.Token, int) {
	token, len := c.parser.TryParse()
	c.err = c.parser.Error()
	if c.err != nil {
		return tokens.New(types.ARRAY, []any{}), 0
	}
	return token, len
}

func (c *redisClient) ProcessRDB() {
	c.parser.ProcessRDB()
	c.err = c.parser.Error()
}
