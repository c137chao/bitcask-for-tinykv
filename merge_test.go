package bitcaskgo

import (
	"bitcask-go/utils"
	"os"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// 没有任何数据的情况下进行 merge
func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-1")
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)
}

// 全部都是有效的数据
func TestDB_Merge2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-2")
	opts.Maxsize = 32 * 1024 * 1024
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := OpenDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 50000, len(keys))

	for i := 0; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

// 有失效的数据，和被重复 Put 的数据
func TestDB_Merge3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-3")
	opts.Maxsize = 32 * 1024 * 1024
	// opts.DataFileMergeRatio = 0
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// fn := func(key []byte, val []byte) bool {
	// 	log.Printf("key %s, val %s", key, val[:4])
	// 	return true
	// }

	// db.Fold(fn)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := OpenDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 40000, len(keys))

	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
	for i := 40000; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("new value in merge"), val)
	}
}

// 全部是无效的数据
func TestDB_Merge4(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-4")
	opts.Maxsize = 32 * 1024 * 1024
	// opts.DataFileMergeRatio = 0
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := OpenDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 0, len(keys))
}

// Merge 的过程中有新的数据写入或删除
func TestDB_Merge5(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-5")
	opts.Maxsize = 32 * 1024 * 1024
	// opts.DataFileMergeRatio = 0
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		logrus.Info("[Test] start Delete 0 ~ 50000")
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		logrus.Info("[Test] finish Delete 0 ~ 50000")
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
		logrus.Info("[Test] Put Key 60000 ~ 70000")

	}()
	logrus.Info("[Test] start merge")
	err = db.Merge()
	logrus.Info("[Test] finish merge")

	assert.Nil(t, err)
	wg.Wait()

	//重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := OpenDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}
