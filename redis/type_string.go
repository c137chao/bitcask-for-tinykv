package redis

import (
	"encoding/binary"
	"log"
	"time"
)

func (rds *RedisStorage) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String

	var expire int64 = 0
	var index = 1
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}

	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)
	log.Printf("BCRedis-String Set %s %s with expire %v\n", key, value, expire)
	return rds.engine.Put(key, encValue)
}

//
func (rds *RedisStorage) Get(key []byte) ([]byte, error) {
	encVal, err := rds.engine.Get(key)
	if err != nil {
		log.Printf("Get error %v", err)
		return nil, err
	}

	// decode value
	datatyp := encVal[0]
	if datatyp != String {
		log.Printf("String Get - Err Wrong Key Type\n")
		return nil, ErrWrongKeyType
	}

	var index = 1
	expire, n := binary.Varint(encVal[index:])
	index += n

	if expire > 0 && expire <= time.Now().UnixNano() {
		log.Printf("BCRedis-String get %s: stale value\n", key)
		return nil, nil
	}

	log.Printf("Get %s -> expire: %v value %s", key, expire, encVal[index:])

	return encVal[index:], nil
}
