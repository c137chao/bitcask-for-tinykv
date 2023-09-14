package index

import (
	"bitcask-go/data"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := newBTree(-1)
	res := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 300})
	assert.True(t, res)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{FileId: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := newBTree(-1)

	res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 300})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.FileId)
	assert.Equal(t, int64(300), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{FileId: 1, Offset: 2})
	assert.True(t, res2)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.FileId)
	assert.Equal(t, int64(2), pos2.Offset)

	res3 := bt.Put([]byte("b"), &data.LogRecordPos{FileId: 1, Offset: 10})
	assert.True(t, res3)

	pos3 := bt.Get([]byte("b"))
	assert.Equal(t, uint32(1), pos3.FileId)
	assert.Equal(t, int64(10), pos3.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := newBTree(-1)

	bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 300})
	bt.Put([]byte("a"), &data.LogRecordPos{FileId: 1, Offset: 2})
	bt.Put([]byte("b"), &data.LogRecordPos{FileId: 1, Offset: 10})

	pos1 := bt.Get(nil)
	assert.True(t, pos1 != nil)

	pos2 := bt.Get([]byte("a"))
	assert.True(t, pos2 != nil)

	pos3 := bt.Get([]byte("b"))
	assert.True(t, pos3 != nil)

	assert.True(t, bt.Delete(nil))
	assert.True(t, bt.Delete(([]byte("a"))))
	assert.True(t, bt.Delete(([]byte("b"))))

	assert.True(t, bt.Get(nil) == nil)
	assert.True(t, bt.Get([]byte("a")) == nil)
	assert.True(t, bt.Get([]byte("b")) == nil)
}

func TestBtree_Iter(t *testing.T) {
	bt := newBTree(-1)

	bt.Put([]byte("aaa"), &data.LogRecordPos{FileId: 1, Offset: 2})
	bt.Put([]byte("bbb"), &data.LogRecordPos{FileId: 1, Offset: 100})
	bt.Put([]byte("ccc"), &data.LogRecordPos{FileId: 1, Offset: 50})
	bt.Put([]byte("ddd"), &data.LogRecordPos{FileId: 1, Offset: 30})

	iter := bt.Iterator(false)

	for iter.Valid() {
		log.Printf("key:%s val:%v\n", iter.Key(), iter.Value())
		iter.Next()
	}

	fmt.Printf("\n")
	iter.Rewind()
	for iter.Valid() {
		log.Printf("key:%s val:%v\n", iter.Key(), iter.Value())
		iter.Next()
	}

	fmt.Printf("\n")
	iter.Seek([]byte("bbb"))
	for iter.Valid() {
		log.Printf("key:%s val:%v\n", iter.Key(), iter.Value())
		iter.Next()
	}

	iter.Close()
}
