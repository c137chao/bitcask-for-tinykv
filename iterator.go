package bitcaskgo

import (
	"bitcask-go/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator
	bitcaskDB *DB
	prefix    []byte
}

func (db *DB) NewIterator(opt IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opt.Reverse)
	iter := &Iterator{
		indexIter: indexIter,
		bitcaskDB: db,
		prefix:    opt.Prefix,
	}
	iter.skipToNext()
	return iter
}

func (iter *Iterator) Rewind() {
	iter.indexIter.Rewind()
	iter.skipToNext()
}

func (iter *Iterator) Seek(key []byte) {
	iter.indexIter.Seek(key)
	iter.skipToNext()
}

func (iter *Iterator) Next() {
	iter.indexIter.Next()
	iter.skipToNext()
}

func (iter *Iterator) Valid() bool {
	return iter.indexIter.Valid()
}

func (iter *Iterator) Key() []byte {
	return iter.indexIter.Key()
}

func (iter *Iterator) Value() ([]byte, error) {
	pos := iter.indexIter.Value()
	iter.bitcaskDB.mu.RLock()
	defer iter.bitcaskDB.mu.RUnlock()
	return iter.bitcaskDB.getValueByPostion(pos)
}

func (iter *Iterator) Close() {
	iter.indexIter.Close()
}

func (iter *Iterator) skipToNext() {
	prefixLen := len(iter.prefix)
	if prefixLen == 0 {
		return
	}

	for ; iter.indexIter.Valid(); iter.indexIter.Next() {
		key := iter.indexIter.Key()
		if prefixLen <= len(key) && bytes.Equal(iter.prefix, key[:prefixLen]) {
			break
		}
	}
}
