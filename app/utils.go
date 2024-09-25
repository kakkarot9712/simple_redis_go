package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

func getOctetFromByte(b byte) string {
	bits := fmt.Sprintf("%b", b)
	for range 8 - len(bits) {
		bits = "0" + bits
	}
	return bits
}

func processPropertyLengthForString(database *[]byte, currentPointer int) (length int, pointer int) {
	pointer = currentPointer
	LengthBits := getOctetFromByte((*database)[pointer])
	lenghtId := LengthBits[:2]
	if lenghtId != "11" {
		if lenghtId == "00" {
			length = int((*database)[pointer])
			pointer++
			return
		} else if lenghtId == "01" {
			lengthBits := LengthBits[2:]
			pointer++
			nextBits := getOctetFromByte((*database)[pointer+1])
			pointer++
			lengthBits += nextBits
			len, err := strconv.ParseUint(lengthBits, 2, 32)
			if err != nil {
				log.Fatal("LEN_Conv_01")
			}
			length = int(len)
			pointer += 2
			return
		} else {
			// lenghtId == "10"
			pointer++
			lengthBytes := (*database)[pointer : pointer+4]
			pointer += 4
			length = int(binary.BigEndian.Uint32(lengthBytes))
			return
		}
	} else {
		log.Fatal("strigified int not supported")
		return
	}
}

func processPropertyValueForInt(database *[]byte, currentPointer int) (val int, pointer int) {
	pointer = currentPointer
	LengthBits := getOctetFromByte((*database)[pointer])
	lenghtId := LengthBits[:2]
	if lenghtId == "11" {
		valId := (*database)[pointer]
		if valId == byte(0xC0) {
			pointer++
			val = int((*database)[pointer])
			pointer++
			return
		} else if valId == byte(0xC1) {
			pointer++
			val = int(binary.LittleEndian.Uint16(((*database)[pointer : pointer+2])))
			pointer += 2
			return
		} else if valId == byte(0xC2) {
			pointer++
			val = int(binary.LittleEndian.Uint32((*database)[pointer : pointer+3]))
			pointer += 3
			return
		} else {
			log.Fatal("LZF_NOSUPPORT")
			return
		}
	} else {
		log.Fatal("pure string not supported")
		return
	}
}

func ParseArg(arg config) (string, bool) {
	argIndex := slices.Index(os.Args, string("--"+arg))
	if argIndex == -1 {
		return "", false
	}
	if len(os.Args) < argIndex+2 {
		panic("--" + arg + " does not have any parameters passed!")
	}
	return os.Args[argIndex+1], true
}

func getValueFromDB(key string, rdbKeys *map[string]Value) string {
	val := (*rdbKeys)[key]
	exp := val.Exp
	// updatedAt := val.UpdatedAt
	if exp == 0 {
		return val.Data
		// conn.Write(Encode(val.Data, BULK_STRING))
	} else {
		currentTime := time.Now().UnixMilli()
		if currentTime > int64(exp) {
			delete((*rdbKeys), key)
			return ""
			// conn.Write(Encode("", BULK_STRING))
		} else {
			return val.Data
			// conn.Write(Encode(val.Data, BULK_STRING))
		}
	}
}

func setValueToDB(args []string, rdbKeys *map[string]Value) bool {
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
	return true
}
