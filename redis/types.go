package redis

import (
	bitcaskgo "bitcask-go"
	"errors"
	"time"
)

type RedisStorageType = byte

const (
	String RedisStorageType = iota
	Hash
	Set
	List
	ZSet
)

var ErrWrongKeyType = errors.New("Redis Wrong Key Type")
var ErrNilValue = errors.New("Type error, Nil Value")

type RedisStorage struct {
	engine *bitcaskgo.DB // storage egine
}

func NewRedisStorage(opts bitcaskgo.Options) (*RedisStorage, error) {
	engine, err := bitcaskgo.OpenDB(opts)
	if err != nil {
		return nil, err
	}
	return &RedisStorage{engine: engine}, nil
}

// close redis
func (rds *RedisStorage) Close() error {
	return rds.engine.Close()
}

// delete key
func (rds *RedisStorage) Del(key []byte) error {
	return rds.engine.Delete(key)
}

// type of key: string, hash, set, list, zset
func (rds *RedisStorage) Type(key []byte) (RedisStorageType, error) {
	encVal, err := rds.engine.Get(key)
	if err != nil {
		return 0, err
	}

	if len(encVal) == 0 {
		return 0, ErrNilValue
	}

	return encVal[0], nil
}

// find metadata of key
// if it doesn't exist, init one and return
func (rds *RedisStorage) findMetaData(key []byte, datatype RedisStorageType) (*metadata, error) {
	metaBuf, err := rds.engine.Get(key)
	if err != nil && err != bitcaskgo.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true

	if err == bitcaskgo.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetaData(metaBuf)
		if meta.dataType != datatype {
			return nil, ErrWrongKeyType
		}

		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = true
		}
	}

	if !exist {
		meta = &metadata{
			dataType: datatype,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if datatype == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}

	return meta, nil
}
