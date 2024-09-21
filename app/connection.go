package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func handleConn(conn net.Conn, currentConfig *map[config]string, rdbKeys *map[string]Value, info *map[infoSection]map[string]string, activereplicaConn *[]net.Conn) {
	defer conn.Close()
	emptyRdbBytes := []byte{
		0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xFA, 0x09, 0x72,
		0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2E, 0x32,
		0x2E, 0x30, 0xFA, 0x0A, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x62, 0x69,
		0x74, 0x73, 0xC0, 0x40, 0xFA, 0x05, 0x63, 0x74, 0x69, 0x6D, 0x65, 0xC2,
		0x6D, 0x08, 0xBC, 0x65, 0xFA, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2D, 0x6D,
		0x65, 0x6D, 0xC2, 0xB0, 0xC4, 0x10, 0x00, 0xFA, 0x08, 0x61, 0x6F, 0x66,
		0x2D, 0x62, 0x61, 0x73, 0x65, 0xC0, 0x00, 0xFF, 0xF0, 0x6E, 0x3B, 0xFE,
		0xC0, 0xFF, 0x5A, 0xA2}
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
			commands := ParseCommand(resp[:n])

			for _, c := range commands {
				cmd := c.cmd
				args := c.args
				switch cmd {
				case PING:
					conn.Write(Encode("PONG", SIMPLE_STRING))
				case ECHO:
					msg := strings.Join(args, " ")
					conn.Write(Encode(msg, BULK_STRING))
				case SET:
					setValueToDB(args, rdbKeys)
					conn.Write(Encode("OK", SIMPLE_STRING))
					go func() {
						if activereplicaConn != nil {
							commands := []string{string(SET)}
							commands = append(commands, args...)
							for _, conn := range *activereplicaConn {
								_, err := conn.Write(Encode(commands, ARRAYS))
								if err != nil {
									fmt.Println("replica propagation failed!")
								}
							}
						}
					}()
				case GET:
					value := getValueFromDB(args, rdbKeys)
					conn.Write(Encode(value, BULK_STRING))
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

				case PSYNC:
					if len(args) < 2 {
						conn.Write(Encode("atleast 2 arg is required for PSYNC command", BULK_STRING))
						return
					}
					replId := args[0]
					replOffset := args[1]
					resp := []string{"FULLRESYNC"}
					if replId == "?" {
						resp = append(resp, infoMap[REPLICATION]["master_replid"])
					}
					if replOffset == "-1" {
						resp = append(resp, infoMap[REPLICATION]["master_repl_offset"])
					}
					conn.Write(Encode(strings.Join(resp, " "), SIMPLE_STRING))
					content := []byte{}
					content = append(content, []byte("$88\r\n")...)
					content = append(content, emptyRdbBytes...)
					fmt.Println(*activereplicaConn, "CONN")
					if activereplicaConn != nil {
						(*activereplicaConn) = append((*activereplicaConn), conn)
					}
					conn.Write(content)

				case WAIT:
					if len(args) < 2 {
						conn.Write(Encode("atleast 2 arg is required for WAIT command", BULK_STRING))
						return
					}
					conn.Write(Encode(len(*activereplicaConn), INTEGER))

				default:
					conn.Write(Encode("", BULK_STRING))
				}

			}
		}
	}
}
