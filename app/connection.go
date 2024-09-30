package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type stream struct {
	Tid      uint
	Sequence uint
	key      string
	value    string
}

func handleConn(conn RedisConn) {
	defer conn.Close()
	streams := map[string][]stream{}
	// stream-key { stramId: {key:value} }
	// stream_key 1-1
	// 1-1 foo = bar
	// streams := map[string][]map[string]string{}
	// streams := map[string]string{}
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
			cmd, args := ParseCommand(resp[:n])

			// for _, c := range commands {
			switch cmd {
			case PING:
				conn.SendMessage("PONG", SIMPLE_STRING)
			case ECHO:
				msg := strings.Join(args, " ")
				conn.SendMessage(msg, BULK_STRING)
			case SET:
				setValueToDB(args)
				conn.WriteOK()
				go func() {
					if infoMap[REPLICATION]["role"] == "master" {
						commands := []string{string(SET)}
						commands = append(commands, args...)
						for _, conn := range activeReplicaConn {
							_, err := conn.SendMessage(commands, ARRAYS)
							if err != nil {
								fmt.Println("replica propagation failed!")
							}
						}
					}
				}()
			case GET:
				value := getValueFromDB(args[0])
				conn.SendMessage(value, BULK_STRING)
			case CONFIG:
				if len(args) == 0 {
					conn.SendError("ERR wrong number of args for `config` command")
					continue
				}
				subcmd := strings.ToLower(args[0])
				if subcmd == "get" {
					if len(args) < 2 {
						conn.SendError("ERR key is required for `config get command`")
						continue
					}
					configName := args[1]
					configVal := activeConfig[config(configName)]
					conn.SendMessage([]string{configName, configVal}, ARRAYS)
				}
			case KEYS:
				if len(args) == 0 {
					conn.SendError("ERR wrong number of args for `keys` command")
					continue
				}
				arg := args[0]
				if arg == "*" {
					keys := []string{}
					for key := range storedKeys {
						keys = append(keys, key)
					}
					conn.SendMessage(keys, ARRAYS)
				}
			case INFO:
				if len(args) == 0 {
					conn.SendError("ERR wrong number of args for `info` command")
					continue
				}
				arg := args[0]
				if arg == string(REPLICATION) {
					infoParts := []string{"# Replication"}
					replInfo := infoMap[REPLICATION]

					for key, val := range replInfo {
						infoParts = append(infoParts, key+":"+val)
					}
					conn.SendMessage(strings.Join(infoParts, "\n"), BULK_STRING)
				} else {
					conn.SendError("ERR only REPLICATION is supported for `info` command as of now")
				}
			case REPLCONF:
				if len(args) < 2 {
					conn.SendError("ERR wrong number of args for `replconf` command")
					continue
				}
				confType := args[0]
				if confType == "listening-port" {
					// fmt.Println("REPL PORT:", args[1])
					conn.WriteOK()
				} else if confType == "capa" {
					// fmt.Println("REPL Capabilities: ", args[1:])
					conn.WriteOK()
				} else {
					conn.SendError("ERR invalid sub command received for `replconf`")
				}

			case PSYNC:
				if len(args) < 2 {
					conn.SendError("ERR wrong number of args for `psync` command")
					continue
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
				conn.SendMessage(strings.Join(resp, " "), SIMPLE_STRING)
				content := []byte{}
				content = append(content, []byte("$88\r\n")...)
				content = append(content, emptyRdbBytes...)
				if infoMap[REPLICATION]["role"] == "master" {
					activeReplicaConn = append(activeReplicaConn, conn)
				}
				conn.Write(content)

			case WAIT:
				if len(args) < 2 {
					conn.SendError("ERR wrong number of args for `wait` command")
					continue
				}
				conn.SendMessage(len(activeReplicaConn), INTEGER)
			case TYPE:
				if len(args) < 1 {
					conn.SendError("ERR wrong number of args for `type` command")
					continue
				}
				key := args[0]
				value := getValueFromDB(key)
				if value == "" {
					val := streams[key]
					if len(val) > 0 {
						conn.SendMessage("stream", SIMPLE_STRING)
					} else {
						conn.SendMessage("none", SIMPLE_STRING)
					}
				} else {
					conn.SendMessage("string", SIMPLE_STRING)
				}
			case XADD:
				stramKey := args[0]
				id := args[1]
				key := args[2]
				value := args[3]
				currentStreams := streams[stramKey]

				if id == "*" {
					tid := time.Now().UnixMilli()
					var sid uint = 0
					if len(currentStreams) > 0 {
						lastStream := currentStreams[len(currentStreams)-1]
						if lastStream.Tid == uint(tid) {
							sid = lastStream.Sequence + 1
						}
					}
					streams[stramKey] = append(streams[stramKey], stream{Tid: uint(tid), Sequence: uint(sid), key: key, value: value})
					id = fmt.Sprintf("%v-%v", tid, sid)
				} else {
					splits := strings.Split(id, "-")
					tidToCheck := splits[0]
					sidToCheck := splits[1]
					if tidToCheck == "0" && sidToCheck == "0" {
						conn.SendError("ERR The ID specified in XADD must be greater than 0-0")
						continue
					}
					var nsidToCheck uint64
					var ntidToCheck uint64
					if ntidToCheck, err = strconv.ParseUint(tidToCheck, 10, 64); err != nil {
						conn.SendError("ERR invalid Id specified in XADD")
						continue
					}
					if sidToCheck == "*" {
						// Create appropriate sequence id
						if len(currentStreams) > 0 {
							lastPair := streams[stramKey][len(streams[stramKey])-1]
							tid := lastPair.Tid
							sid := lastPair.Sequence
							if tid == uint(ntidToCheck) {
								streams[stramKey] = append(streams[stramKey], stream{Tid: uint(ntidToCheck), Sequence: sid + 1, key: key, value: value})
								id = fmt.Sprintf("%v-%v", ntidToCheck, sid+1)
							} else {
								streams[stramKey] = append(streams[stramKey], stream{Tid: uint(ntidToCheck), Sequence: 0, key: key, value: value})
								id = fmt.Sprintf("%v-%v", ntidToCheck, 0)
							}
						} else {
							streams[stramKey] = append(streams[stramKey], stream{Tid: uint(ntidToCheck), Sequence: 1, key: key, value: value})
							id = fmt.Sprintf("%v-%v", ntidToCheck, 1)
						}
					} else {
						if nsidToCheck, err = strconv.ParseUint(sidToCheck, 10, 64); err != nil {
							conn.SendError("ERR invalid Id specified in XADD")
							continue
						}
						if len(currentStreams) > 0 {
							lastPair := streams[stramKey][len(streams[stramKey])-1]
							tid := lastPair.Tid
							sid := lastPair.Sequence
							if tid == uint(ntidToCheck) && sid == uint(nsidToCheck) || tid == uint(ntidToCheck) && sid > uint(nsidToCheck) || tid > uint(ntidToCheck) {
								conn.SendError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
								continue
							}
						}
						streams[stramKey] = append(streams[stramKey], stream{key: key, Tid: uint(ntidToCheck), Sequence: uint(nsidToCheck), value: value})
					}
				}
				conn.SendMessage(id, BULK_STRING)
			default:
				conn.SendError("ERR unsupported Command received")
			}
			// }
		}
	}
}
