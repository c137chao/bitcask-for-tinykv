package redis

import (
	bitcaskgo "bitcask-go"
	"encoding/binary"
)

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	var index = 0
	index += copy(buf[index:], hk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(hk.version))
	index += 8
	index += copy(buf[index:], hk.field)

	return buf
}

func (rds *RedisStorage) HSet(key, field, value []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	encKey := hk.encode()

	var exist = true
	if _, err = rds.engine.Get(encKey); err == bitcaskgo.ErrKeyNotFound {
		exist = false
	}
	wb := rds.engine.NewWriteBatch(bitcaskgo.DefaultWriteBatchOptions)
	// find
	if !exist {
		meta.size += 1
		_ = wb.Put(key, meta.encode())
	}

	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisStorage) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.engine.Get(hk.encode())
}

func (rds *RedisStorage) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	encKey := hk.encode()

	var exist = true
	if _, err = rds.engine.Get(encKey); err == bitcaskgo.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.engine.NewWriteBatch(bitcaskgo.DefaultWriteBatchOptions)
		meta.size -= 1
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

func (rds *RedisStorage) HGetAll(key []byte) ([][]byte, error) {
	panic("unsupport opertion hash get all")
}

func (rds *RedisStorage) HGetExist(key, field []byte) (bool, error) {
	panic("unsupport opertion hash get exist")
}
