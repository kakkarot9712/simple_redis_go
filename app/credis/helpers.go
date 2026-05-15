package credis

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

func GenerateString(size uint) string {
	if size == 0 {
		return ""
	}
	allowedChars := "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var str strings.Builder
	for range size {
		index := rand.Intn(int(len(allowedChars)))
		str.WriteString(string(allowedChars[index]))
	}
	return str.String()
}

func EncodeError(err error, enc Encoder) (bool, []byte) {
	if err == nil {
		return false, nil
	}
	return true,
		enc.SimpleError(err.Error())
}

func SHA256Hex(raw string) string {
	sum := sha256.New()
	var buff bytes.Buffer
	enc := hex.NewEncoder(&buff)
	sum.Write([]byte(raw))
	enc.Write(sum.Sum(nil))
	return buff.String()
}

func FlipCoin(bias float32) bool {
	lose := bias * 100
	flipped := rand.Intn(101)
	return flipped > int(lose)
}

func GetTsAndSeq(id string) (int64, *int64, error) {
	ids := strings.Split(id, "-")
	var ts int64
	var seq *int64
	if len(ids) == 2 {
		for index, id := range ids {
			if index == 1 && id == "*" {
				continue
			}
			if val, err := strconv.ParseInt(id, 10, 64); err != nil {
				return 0, nil, fmt.Errorf("invalid stream id for command XADD")
			} else {
				if index == 0 {
					ts = val
				} else {
					seq = &val
				}
			}
		}
	} else if val, err := strconv.ParseInt(id, 10, 64); err != nil {
		return 0, nil, err
	} else {
		ts = val
	}
	return ts, seq, nil
}
