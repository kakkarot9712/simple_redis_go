package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func handleConn(conn net.Conn) {
	defer conn.Close()
	store := map[string]Value{}
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
				val := Value{}
				withPxArg := false
				if strings.ToLower(args[len(args)-2]) == "px" {
					exp, err := strconv.Atoi(args[len(args)-1])
					if err == nil {
						val.Exp = miliseconds(exp)
						val.Data = strings.Join(args[1:len(args)-2], " ")
						val.UpdatedAt = time.Duration(time.Now().UnixMilli())
						withPxArg = true
					}
				}
				if !withPxArg {
					val.Data = strings.Join(args[1:], " ")
				}
				store[key] = val
				conn.Write(Encode([]byte("OK"), SIMPLE_STRING))
			case GET:
				key := args[0]
				val := store[key]
				exp := val.Exp
				updatedAt := val.UpdatedAt

				if exp == 0 {
					conn.Write(Encode([]byte(val.Data), BULK_STRING))
				} else {
					currentTime := time.Now().UnixMilli()
					expTime := updatedAt + time.Duration(exp)
					if currentTime > int64(expTime) {
						delete(store, key)
						conn.Write(Encode([]byte(""), BULK_STRING))
					} else {
						conn.Write(Encode([]byte(val.Data), BULK_STRING))
					}
				}
			default:
				conn.Write(Encode([]byte("USPCMD"), SIMPLE_STRING))
			}
		}
	}
}
