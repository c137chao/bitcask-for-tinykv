package redis

import (
	bitcaskgo "bitcask-go"
	"encoding/binary"
)

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)
	var index = 0

	index += copy(buf[index:], sk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(sk.version))
	index += 8

	index += copy(buf[index:], sk.member)
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

// set has no value
func (rds *RedisStorage) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = rds.engine.Get(sk.encode()); err == bitcaskgo.ErrKeyNotFound {
		wb := rds.engine.NewWriteBatch(bitcaskgo.DefaultWriteBatchOptions)
		meta.size += 1
		wb.Put(key, meta.encode()) // update meta data
		wb.Put(sk.encode(), nil)   //

		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil

}

func (rds *RedisStorage) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.engine.Get(sk.encode())
	if err != nil && err != bitcaskgo.ErrKeyNotFound {
		return false, err
	}
	if err == bitcaskgo.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// set remove
func (rds *RedisStorage) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.engine.Get(sk.encode()); err == bitcaskgo.ErrKeyNotFound {
		return false, nil
	}

	wb := rds.engine.NewWriteBatch(bitcaskgo.DefaultWriteBatchOptions)
	meta.size -= 1
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
