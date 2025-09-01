package credis

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/credis/commands"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"
)

func handle(conn Conn) {
	for {
		token, _ := conn.Parser.TryParse()
		if conn.Parser.Error() != nil {
			// TODO: Actual Error
			if errors.Is(conn.Parser.Error(), io.EOF) {
				// Connection is closed
				break
			}
		}
		tokenType := token.Type
		switch tokenType {
		case types.ARRAY:
			tokens := token.Literal.([]tokens.Token)
			if len(tokens) > 0 {
				command := tokens[0]
				c := strings.ToLower(command.Literal.(string))
				conn.Executor.Exec(c, tokens[1:])
				if conn.Executor.Error() != nil {
					// TODO: Something is wrong
					fmt.Println("error while execution: ", conn.Executor.Error())
				}
				conn.Conn.Write(conn.Executor.Out())
				// Propagate to replicas
				switch c {
				case commands.SET, commands.INCR:
					conn.replHandler.PropagateToReplicaGroup(tokens...)
				}
				if conn.Executor.VerifiedReplica() && !conn.replHandler.IsPartOfReplicaGroup(conn.Id) {
					conn.replHandler.AddToReplicaGroup(conn)
				}
			}
		default:
			fmt.Println("unsupported command pattern: ", token)
		}
	}
}
