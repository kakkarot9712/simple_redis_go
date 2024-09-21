package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

type protospecs string

const (
	SIMPLE_STRING    protospecs = "+"
	SIMPLE_ERRORS    protospecs = "-"
	INTEGER          protospecs = ";"
	BULK_STRING      protospecs = "$"
	ARRAYS           protospecs = "*"
	NULLS            protospecs = "_"
	BOOLEANS         protospecs = "#"
	DOUBLES          protospecs = ","
	BIG_NUMBERS      protospecs = "("
	BULK_ERRORS      protospecs = "!"
	VERBATIM_STRINGS protospecs = "="
	MAPS             protospecs = "%"
	SETS             protospecs = "~"
	PUSHES           protospecs = ">"
)

type command string

const (
	UNSUPPORTED command = "na"
	PING        command = "ping"
	ECHO        command = "echo"
	SET         command = "set"
	GET         command = "get"
	CONFIG      command = "config"
	KEYS        command = "keys"
	INFO        command = "info"
	REPLCONF    command = "replconf"
	PSYNC       command = "psync"
)

type config string

const (
	DIR        config = "dir"
	DBFILENAME config = "dbfilename"
	PORT       config = "port"
	ReplicaOf  config = "replicaof"
)

type miliseconds uint64

type Value struct {
	Data string
	Exp  miliseconds
}

type tablesubsection uint

const (
	DB_START    tablesubsection = 254
	DB_METADATA tablesubsection = 250
)

type infoSection string

const (
	REPLICATION infoSection = "replication"
)

var infoMap = map[infoSection]map[string]string{
	REPLICATION: {
		"role":               "master",
		"master_replid":      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		"master_repl_offset": "0",
	},
}

var SupportedInfoSections = []infoSection{REPLICATION}
var SupportedCommands = []command{PING, ECHO, SET, GET, CONFIG, KEYS, INFO, REPLCONF, PSYNC}
var SupportedConfigs = []config{DIR, DBFILENAME, PORT, ReplicaOf}

var defaultConfig = map[config]string{DIR: "/tmp/redis-files", DBFILENAME: "dump.rdb", PORT: "6379"}

func proccessArgs() map[config]string {
	configs := defaultConfig
	for _, conf := range SupportedConfigs {
		configVal, found := ParseArg(conf)
		if found {
			configs[conf] = configVal
		}
		if conf == ReplicaOf && configVal != "" {
			infoMap[REPLICATION]["role"] = "slave"
		}
	}
	return configs
}

func loadRedisDB(filepath string, filename string) map[string]Value {
	MagicNumber := "REDIS0011"
	storedKeys := make(map[string]Value)
	buffer, err := os.ReadFile(path.Join(filepath, filename))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Println("Database File Open Failed! ignoring...")
		return storedKeys
	}
	if len(buffer) == 0 {
		return storedKeys
	}
	if len(buffer) < 9 {
		fmt.Println("Database corrupted! ignoring...")
		return storedKeys
	}
	if string(buffer[:9]) != MagicNumber {
		fmt.Println("File is not rdb file! ignoring...")
		return storedKeys
	}

	// TODO: Perform CRC Checksum of whole file
	// crcChecksum := buffer[len(buffer)-8:]
	// fmt.Println(crcChecksum)

	// TODO: Meatdata Section (FA)

	// Find Database index directly
	databaseIndex := bytes.Index(buffer, []byte{254})
	if databaseIndex == -1 {
		return storedKeys
	}

	// Database Section (FE)
	database := buffer[databaseIndex:]
	// index := database[1]
	keyValNums := 0
	if database[2] == 251 {
		keyValNums = int(database[3])
		// expiresNum := uint(database[4])
	}
	pointer := 5
	gotKVp := 0
	key := ""
	val := Value{}
	for {
		b := database[pointer]
		if database[pointer] == 252 || database[pointer] == 253 {
			// key Has expiry
			if database[pointer] == 252 {
				pointer++
				exp := binary.LittleEndian.Uint64(database[pointer : pointer+8])
				val.Exp = miliseconds(exp)
				pointer += 8
			} else {
				pointer++
				exp := binary.LittleEndian.Uint32(database[pointer : pointer+4])
				val.Exp = miliseconds(exp * 1000)
				pointer += 4
			}
		} else if b == 0 {
			// String
			pointer++
			keyLengthBits := getOctetFromByte(database[pointer])
			if keyLengthBits[:2] != "11" {
				keyLength, newPointer := processPropertyLengthForString(&database, pointer)
				pointer = newPointer
				key = string(database[pointer : pointer+int(keyLength)])
				pointer += int(keyLength)
			} else {
				keyint, newPointer := processPropertyValueForInt(&database, pointer)
				key = strconv.Itoa(keyint)
				pointer = newPointer
			}
			valLenBits := getOctetFromByte(database[pointer])
			if valLenBits[:2] != "11" {
				valLength, newPointer := processPropertyLengthForString(&database, pointer)
				pointer = newPointer
				val.Data = string(database[pointer : pointer+valLength])
				pointer += valLength
			} else {
				valInt, newPointer := processPropertyValueForInt(&database, pointer)
				val.Data = strconv.Itoa(valInt)
				pointer = newPointer
			}

			// Omit key-value if it is expired
			if val.Exp == 0 || val.Exp > miliseconds(time.Now().UnixMilli()) {
				storedKeys[key] = val
			} else {
				fmt.Println("key with name", key, "is ommited as it was expired")
			}
			key = ""
			val = Value{}
			gotKVp++
		} else {
			pointer++
		}
		if b == byte(0xFF) {
			if gotKVp == keyValNums {
				return storedKeys
			} else {
				fmt.Println("incomplete Data found, number of keys mistmatch. restore is aborted.")
				return make(map[string]Value)
			}
		}
	}
}

type redisCommand struct {
	cmd  command
	args []string
}

func ParseCommand(buffer []byte) []redisCommand {
	chunks := strings.Split(string(buffer), "*")
	redisCommands := []redisCommand{}
	for _, chunk := range chunks[1:] {
		if len(chunk) < 3 {
			continue
		}
		encodedBytes := []byte("*")
		data := Decode(append(encodedBytes, []byte(chunk)...))
		if len(chunk) < 3 {
			continue
		}
		cmds, ok := data.([]string)
		if !ok || len(cmds) < 1 {
			continue
			// log.Fatal("BadData PCMD")
		}
		commandData := redisCommand{}
		c := command(strings.ToLower(cmds[0]))
		if slices.Contains(SupportedCommands, c) {
			commandData.cmd = c
			if c == KEYS {
				commandData.args = []string{"*"}
			} else {
				commandData.args = cmds[1:]
			}
		} else {
			commandData.cmd = UNSUPPORTED
		}
		redisCommands = append(redisCommands, commandData)
	}
	return redisCommands
}

func Encode(dec any, spec protospecs) []byte {
	switch spec {
	case BULK_STRING:
		decstr, ok := dec.(string)
		if !ok {
			log.Fatalf("Invalid dec passed! expected string got %T", decstr)
		}
		if dec == "" {
			return []byte(string(BULK_STRING) + "-1" + "\r\n")
		}
		return []byte(string(BULK_STRING) + strconv.Itoa(len(decstr)) + "\r\n" + decstr + "\r\n")
	case SIMPLE_STRING:
		decstr, ok := dec.(string)
		if !ok {
			log.Fatalf("Invalid dec passed! expected string got %T", dec)
		}
		return []byte(string(SIMPLE_STRING) + decstr + "\r\n")
	case ARRAYS:
		decarr, ok := dec.([]string)
		if !ok {
			log.Fatalf("Invalid dec passed! expected array of strings got %T", dec)
		}
		enc := string(ARRAYS) + strconv.Itoa(len(decarr)) + "\r\n"
		for _, d := range decarr {
			encBS := Encode(d, BULK_STRING)
			enc += string(encBS)
		}
		return []byte(enc)
	default:
		log.Fatal("Yet to implement!")
	}
	return []byte{}
}

func Decode(enc []byte) any {
	strType := enc[0]
	chunks := bytes.Split(enc[1:], []byte("\r\n"))
	switch protospecs(strType) {
	case SIMPLE_STRING:
		dec := string(chunks[0])
		return dec

	case BULK_STRING:
		_, err := strconv.Atoi(string(chunks[0]))
		if err != nil {
			log.Fatal("BLEN", err)
		}
		_ = chunks[1]
		// if len(content) != contentLength {
		// 	log.Fatal("Content Validation failed! Length mismatch: expacted " + strconv.Itoa(contentLength) + " got " + strconv.Itoa(len(content)))
		// }
		return string(chunks[1])

	case ARRAYS:
		// fmt.Println(string(enc))
		arrayLength, err := strconv.Atoi(string(chunks[0]))
		if err != nil {
			log.Fatal("ARLEN", err)
		}
		cursor := 1
		processed := 0
		data := []string{}
		for {
			if protospecs(chunks[cursor][0]) == BULK_STRING {
				clrfBytes := []byte("\r\n")
				encData := chunks[cursor]
				encData = append(encData, clrfBytes...)
				encData = append(encData, chunks[cursor+1]...)
				dec := Decode(encData)
				val, ok := dec.(string)
				if ok {
					data = append(data, val)
				} else {
					log.Fatal("BadData Received!: ", dec)
				}
				cursor += 2
				processed++
			}

			if processed == arrayLength {
				break
			}
		}
		return data

	default:
		log.Fatal("Invalid start of data: " + string(enc[0]))
		return ""
	}
}

func handleReplicaConnection(url string, port string, storedKeys *map[string]Value) {
	conn, err := net.Dial("tcp", url)
	if err != nil {
		fmt.Println("Failed to connect to master", url)
		os.Exit(1)
	}
	// Perform Handshak process
	conn.Write(Encode([]string{"PING"}, ARRAYS))
	buff := make([]byte, 512)
	okReceived := 0
	waitingForPSYNC := false
	handshakeCompleted := false
	for {
		size, err := conn.Read(buff)
		if err != nil && !errors.Is(err, io.EOF) {
			fmt.Println("[HANDSHAKE] Error reading buffer: ", err.Error())
			os.Exit(1)
		}
		if size == 0 {
			continue
		}
		if string(buff[:size]) == "+PONG\r\n" {
			_, err := conn.Write(Encode([]string{string(REPLCONF), "listening-port", port}, ARRAYS))
			if err != nil {
				log.Fatal("[HANDSHAKE 1] Error writing REPLCONF command")
			}
			_, err = conn.Write(Encode([]string{string(REPLCONF), "capa", "psync2"}, ARRAYS))
			if err != nil {
				log.Fatal("[HANDSHAKE 2] Error writing REPLCONF command")
			}
			continue
		}
		if string(buff[:size]) == "+OK\r\n" {
			okReceived++
		}
		if waitingForPSYNC {
			waitingForPSYNC = false
			handshakeCompleted = true
			// +FULLRESYNC <REPL_ID> 0\r\n
			// RedisDB content + commands (maybe)
			// TODO:Load redis DB
			crlfIndex := bytes.Index(buff[:size], []byte("0\r\n"))
			dbEndByteIndex := bytes.Index(buff[crlfIndex+1:size], []byte{255})
			if dbEndByteIndex != -1 && size > dbEndByteIndex+8 {
				commandsBuff := buff[crlfIndex+1+dbEndByteIndex+8+1 : size]
				cmds := ParseCommand(commandsBuff)
				fmt.Println(cmds, "CMDS")
				if len(cmds) > 0 {
					for _, c := range cmds {
						setValueToDB(c.args, storedKeys)
					}
				}
			}
			continue
		}
		if okReceived == 2 {
			_, err := conn.Write(Encode([]string{string(PSYNC), "?", "-1"}, ARRAYS))
			if err != nil {
				log.Fatal("[HANDSHAKE] Error writing REPLCONF command")
			}
			waitingForPSYNC = true
			okReceived = 0
			continue
		}
		if handshakeCompleted {
			commands := ParseCommand(buff[:size])
			for _, c := range commands {
				switch c.cmd {
				case SET:
					setValueToDB(c.args, storedKeys)
				default:
					fmt.Println(c.cmd, "ignored")
				}
			}
		}
		// +FULLRESYNC
	}
}
