package bitcaskgo

import (
	"bitcask-go/index"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var dir, _ = os.MkdirTemp("", "bitcask-go-iteraotr")
var opts = Options{
	DirPath:   dir,
	Maxsize:   256 * 1024 * 1024,
	SyncWrite: false,
	Index:     index.BTREE,
}

func TestDB_EmptyIterator(t *testing.T) {
	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	iter := db.NewIterator(DefaultIterOptions)
	assert.NotNil(t, iter)
	assert.True(t, !iter.Valid())
}

func TestDB_Iterator_Basic(t *testing.T) {
	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put([]byte("key-0"), []byte("value-0"))
	db.Put([]byte("key-1"), []byte("value-1"))
	db.Put([]byte("key-2"), []byte("value-2"))
	db.Put([]byte("key-3"), []byte("value-3"))
	db.Put([]byte("key-4"), []byte("value-4"))

	iter := db.NewIterator(DefaultIterOptions)
	assert.NotNil(t, iter)

	for ; iter.Valid(); iter.Next() {
		val, _ := iter.Value()
		log.Printf("key: %s, val: %s\n", iter.Key(), val)
	}
}

func TestDB_Iterator_Seek(t *testing.T) {
	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put([]byte("key-0"), []byte("value-0"))
	db.Put([]byte("key-1"), []byte("value-1"))
	db.Put([]byte("key-2"), []byte("value-2"))
	db.Put([]byte("key-3"), []byte("value-3"))
	db.Put([]byte("key-4"), []byte("value-4"))

	iter := db.NewIterator(DefaultIterOptions)
	assert.NotNil(t, iter)

	for iter.Seek([]byte("key-2")); iter.Valid(); iter.Next() {
		val, _ := iter.Value()
		log.Printf("key: %s, val: %s\n", iter.Key(), val)
	}
}

func TestDB_Iterator_Prefix(t *testing.T) {
	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put([]byte("usa-newyork"), []byte("value-newyork"))
	db.Put([]byte("ch-guangzhou"), []byte("value-guangzhou"))
	db.Put([]byte("ch-xian"), []byte("value-xian"))
	db.Put([]byte("usa-logangi"), []byte("value-logangi"))
	db.Put([]byte("ch-qingdao"), []byte("value-qingdao"))
	db.Put([]byte("ch-beijing"), []byte("value-beijing"))
	db.Put([]byte("ch-shanghai"), []byte("value-shanghai"))
	db.Put([]byte("usa-washington"), []byte("value-washington"))
	db.Put([]byte("uk-landon"), []byte("value-landon"))
	db.Put([]byte("nl-amsterdam"), []byte("value-amsterdam"))
	db.Put([]byte("uk-oxford"), []byte("value-oxford"))

	iterOpts := DefaultIterOptions
	iterOpts.Prefix = []byte("ch")
	iter := db.NewIterator(iterOpts)
	assert.NotNil(t, iter)

	for ; iter.Valid(); iter.Next() {
		val, _ := iter.Value()
		log.Printf("key: %s, val: %s\n", iter.Key(), val)
	}
}
