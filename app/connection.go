package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func handleConn(conn net.Conn) {
	defer conn.Close()
	store := map[string]string{}
	for {
		resp := make([]byte, 500)
		n, err := conn.Read(resp)
		if err != nil && err != io.EOF {
			fmt.Println("Error Reading from conn: ", err)
			os.Exit(1)
		}
		if n > 0 {
			if err != nil && err != io.EOF {
				fmt.Println("Error Decode: ", err)
				// os.Exit(1)
			}
			cmd, args := ParseCommand(resp)
			switch cmd {
			case PING:
				conn.Write(Encode([]byte("PONG"), SIMPLE_STRING))
			case ECHO:
				msg := strings.Join(args, " ")
				conn.Write(Encode([]byte(msg), BULK_STRING))
			case SET:
				key := args[0]
				val := args[1]
				store[key] = val
				conn.Write(Encode([]byte("OK"), SIMPLE_STRING))
			case GET:
				key := args[0]
				val := store[key]
				conn.Write(Encode([]byte(val), BULK_STRING))
			default:
				conn.Write(Encode([]byte("USPCMD"), SIMPLE_STRING))
			}
		}
	}
}
