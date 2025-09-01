package helpers

import (
	"math/rand"
)

func GenerateString(size uint) string {
	if size == 0 {
		return ""
	}
	allowedChars := "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	str := ""
	for range size {
		index := rand.Intn(int(len(allowedChars)))
		str += string(allowedChars[index])
	}
	return str
}
