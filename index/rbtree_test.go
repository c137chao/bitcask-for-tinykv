package index

import (
	"bitcask-go/data"
	"log"
	"testing"
)

func TestTBTree_Put(t *testing.T) {
	rbt := NewRBTree()
	ans := rbt.Put([]byte("key1"), &data.LogRecordPos{FileId: 1, Offset: 11})
	log.Printf("key1: %v\n", ans)

	ans = rbt.Put([]byte("key1"), &data.LogRecordPos{FileId: 1, Offset: 21})
	log.Printf("key1: %v\n", ans)

	ans = rbt.Put([]byte("key1"), &data.LogRecordPos{FileId: 1, Offset: 31})
	log.Printf("key1: %v\n", ans)

	ans = rbt.Put([]byte("key1"), &data.LogRecordPos{FileId: 1, Offset: 41})
	log.Printf("key1: %v\n", ans)

	ans = rbt.Put([]byte("key1"), &data.LogRecordPos{FileId: 1, Offset: 51})
	log.Printf("key1: %v\n", ans)

	pos := rbt.Get([]byte("key1"))
	log.Printf("key1: %v\n", pos)

}

func TestTBTree_Iter(t *testing.T) {
	rbt := NewRBTree()
	rbt.Put([]byte("key1"), &data.LogRecordPos{FileId: 1, Offset: 11})
	rbt.Put([]byte("key2"), &data.LogRecordPos{FileId: 1, Offset: 21})
	rbt.Put([]byte("key3"), &data.LogRecordPos{FileId: 1, Offset: 31})
	rbt.Put([]byte("key4"), &data.LogRecordPos{FileId: 1, Offset: 41})
	rbt.Put([]byte("key5"), &data.LogRecordPos{FileId: 1, Offset: 51})

	iter := newRBtreeIter(rbt.tree, false)
	for ; iter.Valid(); iter.Next() {
		log.Printf("key %s, val %v", iter.Key(), iter.Value())
	}
}
