package main

import (
	"math/rand"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Gnerate a random string of alphabetic characters.
func randString(r *rand.Rand, length int) string {
	str := make([]byte, length)
	for i := range str {
		str[i] = letters[r.Intn(len(letters))]
	}
	return string(str)
}
