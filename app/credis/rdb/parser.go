package rdb

import (
	"encoding/binary"
	"fmt"
)

func (cfg *rdbStore) string() string {
	strLen := cfg.length()
	if strLen == -1 {
		// Special encoding
		b, err := cfg.reader.ReadByte()
		if err != nil {
			cfg.err = err
			return ""
		}
		switch b & 63 { // 0b00111111
		case 0:
			// 8 bit integer
			num, err := cfg.reader.ReadByte()
			if err != nil {
				cfg.err = err
				return ""
			}
			return fmt.Sprintf("%v", num)
		case 1:
			// 16 bit integer
			numBytes := make([]byte, 2)
			_, err := cfg.reader.Read(numBytes)
			if err != nil {
				cfg.err = err
				return ""
			}
			return fmt.Sprintf("%v", binary.BigEndian.Uint16(numBytes))
		case 2:
			// 32 bit integer
			numBytes := make([]byte, 4)
			_, err := cfg.reader.Read(numBytes)
			if err != nil {
				cfg.err = err
				return ""
			}
			return fmt.Sprintf("%v", binary.BigEndian.Uint32(numBytes))
		default:
			cfg.err = fmt.Errorf("unsupported encoding format")
			return ""
		}
	}
	if cfg.err != nil {
		return ""
	}
	strBytes := make([]byte, strLen)
	_, err := cfg.reader.Read(strBytes)
	if err != nil {
		cfg.err = err
		return ""
	}
	return string(strBytes)
}

func (cfg *rdbStore) length() int {
	length := 0
	reader := cfg.reader
	b, err := reader.ReadByte()
	if err != nil {
		cfg.err = err
		return 0
	}
	switch b >> 6 {
	case 0b00:
		length = int(b)
	case 0b01:
		nextByte, err := reader.ReadByte()
		if err != nil {
			cfg.err = err
			return 0
		}
		lenBytes := make([]byte, 2)
		lenBytes[0] = b & 63 // 0b00111111
		lenBytes[1] = nextByte
		length = int(binary.LittleEndian.Uint16(lenBytes))
	case 0b10:
		lenBytes := make([]byte, 4)
		length = int(binary.BigEndian.Uint32(lenBytes))
	case 0b11:
		length = -1
		reader.UnreadByte()
	}
	return length
}
