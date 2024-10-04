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
	// streamKey => Tid => Sequence => []stream
	streams := map[string]map[uint64]map[uint64][]stream{}
	currentTIndex := 0
	currentSIndex := map[uint64]uint64{}
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
			rcmd, err := ParseCommand(resp[:n])
			if err != nil {
				conn.SendError("ERR Invalid command received")
				continue
			}
			// for _, c := range commands {
			args := rcmd.args
			switch rcmd.cmd {
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
				keyValueArgs := args[2:]
				if id == "*" {
					// find sequence and value ourself

					// Tid is Current time
					tid := time.Now().UnixMilli()

					// Taking sid from mapping
					sid := currentSIndex[uint64(tid)]
					if sid > 0 {
						sid = sid + 1
					}
					ns := stream{
						Tid: uint(tid),
					}

					if streams[stramKey] == nil {
						streams[stramKey] = make(map[uint64]map[uint64][]stream)
					}

					if streams[stramKey][uint64(tid)] == nil {
						streams[stramKey][uint64(tid)] = make(map[uint64][]stream)
					}

					if streams[stramKey][uint64(tid)][sid] == nil {
						streams[stramKey][uint64(tid)][sid] = []stream{}
					}

					for i, kv := range keyValueArgs {
						if i%2 == 0 {
							ns.key = kv
						} else {
							ns.value = kv
							ns.Sequence = uint(sid)
							fmt.Println(streams[stramKey][uint64(tid)][sid])
							streams[stramKey][uint64(tid)][sid] = append(streams[stramKey][uint64(tid)][sid], ns)
							ns = stream{Tid: uint(tid)}
							sid++
						}
					}
					id = fmt.Sprintf("%v-%v", tid, sid)
					currentTIndex = int(tid)
					currentSIndex[uint64(tid)] = sid - 1
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

					// currentStreams := streams[stramKey][ntidToCheck]

					if sidToCheck == "*" {
						// Get sid ourself, but tid is given so validate it first
						if ntidToCheck >= uint64(currentTIndex) {
							// TID is higher then or equal to the Last Acknowledged TID So that is OK
							sequence := currentSIndex[ntidToCheck]
							if ntidToCheck == 0 && sequence == 0 {
								sequence = 1
							}
							ns := stream{
								Tid: uint(ntidToCheck),
							}

							if streams[stramKey] == nil {
								streams[stramKey] = make(map[uint64]map[uint64][]stream)
							}

							if streams[stramKey][uint64(ntidToCheck)] == nil {
								streams[stramKey][uint64(ntidToCheck)] = make(map[uint64][]stream)
							}

							if streams[stramKey][uint64(ntidToCheck)][nsidToCheck] == nil {
								streams[stramKey][uint64(ntidToCheck)][nsidToCheck] = []stream{}
							}

							for i, kv := range keyValueArgs {
								if i%2 == 0 {
									ns.key = kv
								} else {
									ns.value = kv
									ns.Sequence = uint(sequence)
									streams[stramKey][uint64(ntidToCheck)][sequence] = append(streams[stramKey][uint64(ntidToCheck)][sequence], ns)
									ns = stream{Tid: uint(ntidToCheck)}
									sequence++
								}
							}
							id = fmt.Sprintf("%v-%v", ntidToCheck, sequence)
							currentTIndex = int(ntidToCheck)
							currentSIndex[uint64(currentTIndex)] = sequence - 1
						} else {
							conn.SendError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
							continue
						}
					} else {
						// Both are given so validate both
						if nsidToCheck, err = strconv.ParseUint(sidToCheck, 10, 64); err != nil {
							conn.SendError("ERR invalid Id specified in XADD")
							continue
						}
						if currentTIndex <= int(ntidToCheck) && nsidToCheck > currentSIndex[uint64(currentTIndex)] {
							ns := stream{
								Tid: uint(ntidToCheck),
							}

							if streams[stramKey] == nil {
								streams[stramKey] = make(map[uint64]map[uint64][]stream)
							}

							if streams[stramKey][uint64(ntidToCheck)] == nil {
								streams[stramKey][uint64(ntidToCheck)] = make(map[uint64][]stream)
							}

							if streams[stramKey][uint64(ntidToCheck)][nsidToCheck] == nil {
								streams[stramKey][uint64(ntidToCheck)][nsidToCheck] = []stream{}
							}

							for i, kv := range keyValueArgs {
								if i%2 == 0 {
									ns.key = kv
								} else {
									ns.value = kv
									ns.Sequence = uint(nsidToCheck)
									streams[stramKey][uint64(ntidToCheck)][nsidToCheck] = append(streams[stramKey][uint64(ntidToCheck)][nsidToCheck], ns)
									// fmt.Println(streams, "FFF")
									ns = stream{Tid: uint(ntidToCheck)}
									nsidToCheck++
								}
							}
							currentTIndex = int(ntidToCheck)
							currentSIndex[ntidToCheck] = nsidToCheck - 1
							// fmt.Println(currentSIndex, currentTIndex)
						} else {
							conn.SendError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
							continue
						}
					}
				}
				// fmt.Println(keyValueArgs)
				conn.SendMessage(id, BULK_STRING)
				// XRANGE some_key 1526985054069 1526985054079
				// 			[
				//   [
				//     "1526985054069-0",
				//     [
				//       "temperature",
				//       "36",
				//       "humidity",
				//       "95"
				//     ]
				//   ],
				//   [
				//     "1526985054079-0",
				//     [
				//       "temperature",
				//       "37",
				//       "humidity",
				//       "94"
				//     ]
				//   ],
				// ]
			case XRANGE:
				key := args[0]
				start := args[1] // Inclusive
				end := args[2]   // Inclusive
				var (
					startTid int
					startSid int
					endTid   int
					endSid   int
				)
				hasStartSequence := strings.Index(start, "-")
				hasEndSequence := strings.Index(end, "-")
				if hasEndSequence == -1 {
					endSid = 0
					etid, err := strconv.ParseUint(end, 10, 64)
					if err != nil {
						conn.SendError("ERR invalid start time received")
						continue
					} else {
						endTid = int(etid)
					}
				} else {
					etid, err := strconv.ParseUint(end[:hasEndSequence], 10, 64)
					if err != nil {
						conn.SendError("ERR invalid end time received")
						continue
					} else {
						endTid = int(etid)
					}
					esid, err := strconv.ParseUint(end[hasEndSequence+1:], 10, 64)
					if err != nil {
						conn.SendError("ERR invalid end time received")
						continue
					} else {
						endSid = int(esid)
					}
				}

				if hasStartSequence == -1 {
					startSid = 0
					stid, err := strconv.ParseUint(start, 10, 64)
					if err != nil {
						conn.SendError("ERR invalid start time received")
						continue
					} else {
						startTid = int(stid)
					}
				} else {
					stid, err := strconv.ParseUint(start[:hasStartSequence], 10, 64)
					if err != nil {
						conn.SendError("ERR invalid end time received")
						continue
					} else {
						startTid = int(stid)
					}
					stsid, err := strconv.ParseUint(end[hasStartSequence+1:], 10, 64)
					if err != nil {
						conn.SendError("ERR invalid end time received")
						continue
					} else {
						startSid = int(stsid)
					}
				}
				currentStreams := streams[key]
				xRangeArr := []map[string][]stream{}

				startStreams := currentStreams[uint64(startTid)][uint64(startSid)]
				endStreams := currentStreams[uint64(endTid)][uint64(endSid)]
				startKey := fmt.Sprintf("%v-%v", startTid, startSid)
				endKey := fmt.Sprintf("%v-%v", endTid, endSid)
				xRangeArr = append(xRangeArr, map[string][]stream{startKey: startStreams})
				xRangeArr = append(xRangeArr, map[string][]stream{endKey: endStreams})
				fmt.Println(xRangeArr, streams)
			default:
				conn.SendError("ERR unsupported Command received")
			}
			// }
		}
	}
}
