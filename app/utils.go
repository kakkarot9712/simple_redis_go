package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
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
