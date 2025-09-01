package commands

import "github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"

const (
	ECHO     = "echo"
	COMMAND  = "command"
	PING     = "ping"
	SET      = "set"
	GET      = "get"
	INCR     = "incr"
	MULTI    = "multi"
	EXEC     = "exec"
	DISCARD  = "discard"
	INFO     = "info"
	REPLCONF = "replconf"
	PSYNC    = "psync"
	CONFIG   = "config"
	KEYS     = "keys"
)

type Cmd struct {
	Name string
	Args []tokens.Token
}

func New(cmd string, args ...tokens.Token) Cmd {
	return Cmd{
		Name: cmd,
		Args: args,
	}
}
