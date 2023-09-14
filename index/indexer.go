package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

type IndexType = int8

const (
	BTREE IndexType = iota + 1
	ART
	BPLUSTREE
)

// in-memory key dir interface
type Indexer interface {
	// put <key, value> to btree, return true if put success, false else
	Put(key []byte, value *data.LogRecordPos) bool

	// get value with key, value is rid of data, return nil if key doesn't exist
	Get(key []byte) *data.LogRecordPos

	// delete item with key, return true is delete successm false else
	Delete(key []byte) bool

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
	default:
		panic("unsopported index type")
	}
}

type Item struct {
	key []byte
	val *data.LogRecordPos
}

func (it Item) Less(than btree.Item) bool {
	return bytes.Compare(it.key, than.(*Item).key) == -1
}
