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

func handleConn(conn net.Conn, currentConfig *map[config]string, rdbKeys *map[string]Value, info *map[infoSection]map[string]string) {
	defer conn.Close()

	// store := map[string]Value{}
	for {
		resp := make([]byte, 512)
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
				conn.Write(Encode("PONG", SIMPLE_STRING))
			case ECHO:
				msg := strings.Join(args, " ")
				conn.Write(Encode(msg, BULK_STRING))
			case SET:
				key := args[0]
				val := Value{}
				withPxArg := false
				if strings.ToLower(args[len(args)-2]) == "px" {
					exp, err := strconv.Atoi(args[len(args)-1])
					if err == nil {
						val.Exp = miliseconds(time.Now().UnixMilli() + int64(exp))
						val.Data = strings.Join(args[1:len(args)-2], " ")
						// val.UpdatedAt = time.Duration(time.Now().UnixMilli())
						withPxArg = true
					}
				}
				if !withPxArg {
					val.Data = strings.Join(args[1:], " ")
				}
				(*rdbKeys)[key] = val
				conn.Write(Encode("OK", SIMPLE_STRING))
			case GET:
				key := args[0]
				val := (*rdbKeys)[key]
				exp := val.Exp
				// updatedAt := val.UpdatedAt
				if exp == 0 {
					conn.Write(Encode(val.Data, BULK_STRING))
				} else {
					currentTime := time.Now().UnixMilli()
					if currentTime > int64(exp) {
						delete((*rdbKeys), key)
						conn.Write(Encode("", BULK_STRING))
					} else {
						conn.Write(Encode(val.Data, BULK_STRING))
					}
				}
			case CONFIG:
				if len(args) == 0 {
					conn.Write(Encode("atleast 1 arg is required for CONFIG command", BULK_STRING))
					return
				}
				subcmd := strings.ToLower(args[0])
				if subcmd == "get" {
					if len(args) < 2 {
						conn.Write(Encode("name is required for CONFIG GET command", BULK_STRING))
						return
					}
					configName := args[1]
					configVal := (*currentConfig)[config(configName)]
					data := Encode([]string{configName, configVal}, ARRAYS)
					conn.Write(data)
				}
			case KEYS:
				if len(args) == 0 {
					conn.Write(Encode("atleast 1 arg is required for KEY command", BULK_STRING))
					return
				}
				arg := args[0]
				if arg == "*" {
					keys := []string{}
					for key := range *rdbKeys {
						keys = append(keys, key)
					}
					conn.Write(Encode(keys, ARRAYS))
				}
			case INFO:
				if len(args) == 0 {
					conn.Write(Encode("atleast 1 arg is required for INFO command", BULK_STRING))
					return
				}
				arg := args[0]
				if arg == string(REPLICATION) {
					infoParts := []string{"# Replication"}
					replInfo := (*info)[REPLICATION]

					for key, val := range replInfo {
						infoParts = append(infoParts, key+":"+val)
					}

					conn.Write(Encode(strings.Join(infoParts, "\n"), BULK_STRING))
				} else {
					conn.Write(Encode("Only REPLICATION is supported for INFO command", BULK_STRING))
				}
			case REPLCONF:
				if len(args) < 2 {
					conn.Write(Encode("atleast 2 arg is required for REPLCONF command", BULK_STRING))
					return
				}
				confType := args[0]
				if confType == "listening-port" {
					fmt.Println("REPL PORT:", args[1])
					conn.Write(Encode("OK", SIMPLE_STRING))
				} else if confType == "capa" {
					fmt.Println("REPL Capabilities: ", args[1:])
					conn.Write(Encode("OK", SIMPLE_STRING))
				} else {
					conn.Write(Encode("NOTSUPPORTED", SIMPLE_STRING))
				}
			default:
				conn.Write(Encode("", BULK_STRING))
			}
		}
	}
}
