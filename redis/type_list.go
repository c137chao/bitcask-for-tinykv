package redis

import (
	bitcaskgo "bitcask-go"
	"encoding/binary"
)

/******************************
*
* bit cask list push/pop not a real list
* it is simmilar as hash which use key + location as list key
* so ever list elemnt will alloc a Entry in Index
*
**/
type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)
	var index = 0

	index += copy(buf[index:], lk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(lk.version))
	index += 8
	binary.LittleEndian.PutUint64(buf[index:], uint64(lk.index))

	return buf
}

func (rds *RedisStorage) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisStorage) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisStorage) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisStorage) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisStorage) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return 0, err
	}

	// construct list internal key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	wb := rds.engine.NewWriteBatch(bitcaskgo.DefaultWriteBatchOptions)
	meta.size += 1

	if isLeft {
		meta.head -= 1
	} else {
		meta.tail += 1
	}

	wb.Put(key, meta.encode())
	wb.Put(lk.encode(), element)

	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *RedisStorage) popInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.engine.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	meta.size -= 1
	if isLeft {
		meta.head += 1
	} else {
		meta.tail -= 1
	}

	if err = rds.engine.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}
