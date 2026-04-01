package credis

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
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

func EncodeError(err error, enc *Encoder) (bool, []byte) {
	if err == nil {
		return false, nil
	}
	enc.SimpleError(err.Error())
	enc.Commit()
	return true, enc.Bytes()
}

func SHA256Hex(raw string) string {
	sum := sha256.New()
	var buff bytes.Buffer
	enc := hex.NewEncoder(&buff)
	sum.Write([]byte(raw))
	enc.Write(sum.Sum(nil))
	return buff.String()
}
