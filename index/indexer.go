package index

import (
	"bitcask-go/data"
)

type IndexType = int8
type IndexKeyType = []byte
type IndexValueType = *data.LogRecordPos

const (
	BTREE IndexType = iota + 1
	RBTREE
	ARTREE
	BPLUSTREE
)

// in-memory key dir interface
type Indexer interface {
	// put <key, value> to btree, return nil if key doesn't, else old value
	Put(key []byte, value IndexValueType) IndexValueType

	// get value with key, value is rid of data, return nil if key doesn't exist
	Get(key []byte) IndexValueType

	// delete item with key, return old value if delete successm, nil if key doesn't exist
	Delete(key []byte) IndexValueType

	// create a iterator for index
	Iterator(reverse bool) Iterator

	// return item count of index
	Size() int
}

type Iterator interface {
	// reset iterator to begin of container
	Rewind()

	// find the first item which greate equal than key
	Seek(key []byte)

	// set iter to next item
	Next()

	// return true if iterator is valid, false eles
	Valid() bool

	// return key of iterator
	Key() []byte

	// return val of iterator
	Value() *data.LogRecordPos

	// close iterator
	Close()
}

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTREE:
		return newBTree(-1)
	case RBTREE:
		return NewRBTree()
	default:
		panic("unsopported index type")
	}
}
