package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func handleConn(conn net.Conn, currentConfig *map[config]string, rdbKeys *map[string]Value) {
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
				conn.Write(Encode("OK", SIMPLE_STRING))
			case GET:
				key := args[0]
				val := store[key]
				if val.Data == "" {
					val = (*rdbKeys)[key]
				}
				exp := val.Exp
				updatedAt := val.UpdatedAt
				if exp == 0 {
					conn.Write(Encode(val.Data, BULK_STRING))
				} else {
					currentTime := time.Now().UnixMilli()
					expTime := updatedAt + time.Duration(exp)
					if currentTime > int64(expTime) {
						delete(store, key)
						conn.Write(Encode("", BULK_STRING))
					} else {
						conn.Write(Encode(val.Data, BULK_STRING))
					}
				}
			case CONFIG:
				if len(args) == 0 {
					log.Fatal("arg required with config!")
				}
				subcmd := strings.ToLower(args[0])
				if subcmd == "get" {
					if len(args) < 2 {
						log.Fatal("config name is required with config get!")
					}
					configName := args[1]
					configVal := (*currentConfig)[config(configName)]
					if configVal == "" {
						log.Fatal("inavlid config name passed")
					}
					data := Encode([]string{configName, configVal}, ARRAYS)
					conn.Write(data)
				}
			case KEYS:
				if len(args) == 0 {
					log.Fatal("additional arguments required for KEY command")
				}
				arg := args[0]
				if arg == "*" {
					keys := []string{}
					for key := range *rdbKeys {
						keys = append(keys, key)
					}
					conn.Write(Encode(keys, ARRAYS))
				}
			default:
				conn.Write(Encode("", BULK_STRING))
			}
		}
	}
}
