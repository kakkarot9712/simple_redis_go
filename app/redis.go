package main

import (
	"bytes"
	"fmt"
	"log"
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
)

type config string

const (
	DIR        = "dir"
	DBFILENAME = "dbfilename"
)

type miliseconds uint64

type Value struct {
	Data      string
	Exp       miliseconds
	UpdatedAt time.Duration
}

var SupportedCommands = []command{PING, ECHO, SET, GET, CONFIG}

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
