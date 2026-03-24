package credis

import (
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
