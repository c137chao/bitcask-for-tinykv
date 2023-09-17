package redis

import (
	bitcaskgo "bitcask-go"
	"bitcask-go/utils"
	"encoding/binary"
)

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	// key
	var index = 0
	index += copy(buf[index:], zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:], uint64(zk.version))
	index += 8

	// member
	copy(buf[index:], zk.member)

	return buf
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.FloatToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4)

	// key
	var index = 0
	index += copy(buf[index:], zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:], uint64(zk.version))
	index += 8

	// score
	index += copy(buf[index:], scoreBuf)

	// member
	index += copy(buf[index:], zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}

// add memeber to ordered set key
// if member has been exist, delete it and insert it with new score again
func (rds *RedisStorage) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return false, err
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	value, err := rds.engine.Get(zk.encodeWithMember())
	if err != nil && err != bitcaskgo.ErrKeyNotFound {
		return false, err
	}
	if err == bitcaskgo.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// write to storage engine
	wb := rds.engine.NewWriteBatch(bitcaskgo.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.FloatToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisStorage) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.engine.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
