package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"slices"
	"strconv"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

// $<length>\r\n<data>\r\n --> String
// :[<+|->]<value>\r\n --> Int

// func (resp) EncodeFromString(val string) (enc string) {
// 	enc = "$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"
// 	return
// }

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
)

var SupportedCommands = []command{PING, ECHO}

func ParseCommand(buffer []byte) (command, []string) {
	data := Decode(buffer)
	cmds, ok := data.([]string)
	if !ok || len(cmds) < 1 {
		log.Fatal("BadData PCMD")
	}
	cmd := command(strings.ToLower(cmds[0]))
	if slices.Contains(SupportedCommands, cmd) {
		return cmd, cmds[1:]
	} else {
		return UNSUPPORTED, []string{}
	}
}

func Encode(dec []byte, spec protospecs) []byte {
	switch spec {
	case BULK_STRING:
		return []byte(string(BULK_STRING) + strconv.Itoa(len(dec)) + "\r\n" + string(dec) + "\r\n")
	case SIMPLE_STRING:
		return []byte(string(SIMPLE_STRING) + string(dec) + "\r\n")
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
		contentLength, err := strconv.Atoi(string(chunks[0]))
		if err != nil {
			log.Fatal("BLEN")
		}
		content := chunks[1]
		if len(content) != contentLength {
			log.Fatal("Content Validation failed! Length mismatch: expacted " + strconv.Itoa(contentLength) + " got " + strconv.Itoa(len(content)))
		}
		return string(chunks[1])

	case ARRAYS:
		// fmt.Println(string(enc))
		arrayLength, err := strconv.Atoi(string(chunks[0]))
		if err != nil {
			log.Fatal("ARLEN")
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
		fmt.Println(string(enc))
		log.Fatal("Invalid start of data: " + string(enc[0]))
		return ""
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
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
			default:
				conn.Write(Encode([]byte("USPCMD"), SIMPLE_STRING))
			}
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}
