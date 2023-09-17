package bitcaskgo

import (
	"bitcask-go/utils"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch_Basic(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch")
	opts.DirPath = dir

	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	wb.Put([]byte("usa-newyork"), []byte("value-newyork"))
	wb.Put([]byte("ch-guangzhou"), []byte("value-guangzhou"))
	wb.Put([]byte("ch-xian"), []byte("value-xian"))

	log.Printf("[Log Transaction Sequence Number %v]", db.txnSeqNo)
	wb.Commit()

	wb.Put([]byte("usa-losangi"), []byte("value-losangi"))
	wb.Put([]byte("ch-qingdao"), []byte("value-qingdao"))
	wb.Put([]byte("ch-beijing"), []byte("value-beijing"))
	wb.Put([]byte("ch-shanghai"), []byte("value-shanghai"))
	wb.Put([]byte("usa-washington"), []byte("value-washington"))
	wb.Put([]byte("uk-landon"), []byte("value-landon"))
	wb.Put([]byte("nl-amsterdam"), []byte("value-amsterdam"))
	wb.Put([]byte("uk-oxford"), []byte("value-oxford"))

	wb.Delete([]byte("usa-newyork"))
	wb.Delete([]byte("ch-guangzhou"))

	fn := func(key []byte, val []byte) bool {
		log.Printf("key %s, val %s", key, val)
		return true
	}

	err = db.Fold(fn)
	assert.Nil(t, err)

	log.Printf("[Log Transaction Sequence Number %v]", db.txnSeqNo)
	wb.Commit()

	fmt.Printf("\n\n")
	log.Printf("[Log Transaction Sequence Number %v]", db.txnSeqNo)

	db.Put([]byte("default-key"), []byte("default-value"))

	err = db.Fold(fn)
	assert.Nil(t, err)

}

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据之后并不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 删除有效的数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(11), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := OpenDB(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 校验序列号
	assert.Equal(t, uint64(2), db.txnSeqNo)
}

//func TestDB_WriteBatch3(t *testing.T) {
//	opts := DefaultOptions
//	//dir, _ := os.MkdirTemp("", "bitcask-go-batch-3")
//	dir := "/tmp/bitcask-go-batch-3"
//	opts.DirPath = dir
//	db, err := Open(opts)
//	//defer destroyDB(db)
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//
//	keys := db.ListKeys()
//	t.Log(len(keys))
//	//
//	//wbOpts := DefaultWriteBatchOptions
//	//wbOpts.MaxBatchNum = 10000000
//	//wb := db.NewWriteBatch(wbOpts)
//	//for i := 0; i < 500000; i++ {
//	//	err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
//	//	assert.Nil(t, err)
//	//}
//	//err = wb.Commit()
//	//assert.Nil(t, err)
//}
