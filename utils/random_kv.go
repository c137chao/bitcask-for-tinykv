package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func GetTestKey(num int) []byte {
	return []byte(fmt.Sprintf("key-%09d", num))
}

func GetTestValue(k, v int) []byte {
	return []byte(fmt.Sprintf("x %d %d y", k, v))
}

func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Int()%len(letters)]
	}

	return b
}
