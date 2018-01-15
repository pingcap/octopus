package main

import (
	"math/rand"
	"time"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var Rand = rand.New(rand.NewSource(int64(time.Now().UnixNano())))

// Gnerate a random string of alphabetic characters.
func randString(length int) string {
	str := make([]byte, length)
	for i := range str {
		str[i] = letters[Rand.Intn(len(letters))]
	}
	return string(str)
}
