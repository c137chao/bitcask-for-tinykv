package bitcaskgo

import (
	"bitcask-go/utils"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	// defer destroyFile(filepath.Join(opts.DirPath, "*.data"))
	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)

	key := []byte("default-key")
	val := []byte("default-value")
	if err := db.Put(key, val); err != nil {
		panic(err)
	}

	getval, err := db.Get(key)
	assert.Nil(t, err)
	assert.NotNil(t, getval)
	assert.Equal(t, val, getval)

	err = db.Delete(key)
	assert.Nil(t, err)

	_, err = db.Get(key)
	assert.Equal(t, err, ErrKeyNotFound)
}

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		_ = db.Close()
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opts.DirPath = dir
	opts.Maxsize = 64 * 1024 * 1024
	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := OpenDB(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opts.DirPath = dir
	opts.Maxsize = 64 * 1024 * 1024
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)

	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := OpenDB(opts)
	assert.Nil(t, err)

	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = dir
	opts.Maxsize = 64 * 1024 * 1024
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := OpenDB(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-key")
	opts.DirPath = dir

	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)

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

	keys := db.ListKeys()
	for _, key := range keys {
		log.Printf("key is %s\n", key)
	}

}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-key")
	opts.DirPath = dir

	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)

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

	fn := func(key []byte, val []byte) bool {
		log.Printf("key %s, val %s", key, val)
		return true
	}

	err = db.Fold(fn)
	assert.Nil(t, err)

}

func TestDB_FileLock_Open(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-filelock")
	opts.DirPath = dir

	wg := new(sync.WaitGroup)
	wg.Add(2)

	var idx int32 = 0
	go func() {
		db, err := OpenDB(opts)
		defer destroyDB(db)

		i := atomic.AddInt32(&idx, 1)
		t.Logf("%v %v", i, db)
		t.Logf("%v %v", i, err)

		wg.Done()
	}()

	go func() {
		db, err := OpenDB(opts)
		defer destroyDB(db)

		i := atomic.AddInt32(&idx, 1)
		t.Logf("%v %v", i, db)
		t.Logf("%v %v", i, err)
		wg.Done()
	}()

	wg.Wait()

}

func TestDB_FileLock_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-filelock")
	opts.DirPath = dir

	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	db2, err := OpenDB(opts)
	assert.Nil(t, db2)
	assert.Equal(t, err, ErrDataBaseIsUsing)

	db.Close()

	db, err = OpenDB(opts)

	assert.NotNil(t, db)
	assert.Nil(t, err)
}

func TestDB_Memorymap_Open(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-filelock")
	opts.DirPath = dir

	db, _ := OpenDB(opts)
	defer destroyDB(db)

	for i := 0; i < 1000000; i++ {
		db.Put(utils.GetTestKey(i), utils.GetTestValue(i, 1024))
	}
	db.Close()

	now := time.Now()
	db, _ = OpenDB(opts)
	log.Printf("open time with mmap: %v\n", time.Since(now))

	db.Close()

	opts.MMapAtStartup = false

	now = time.Now()
	db, _ = OpenDB(opts)
	log.Printf("open time with standardio: %v\n", time.Since(now))

	db.Close()
}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-stat")
	opts.DirPath = dir

	db, err := OpenDB(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 10000; i++ {
		db.Put(utils.GetTestKey(i), utils.GetTestValue(i, 1024))
	}

	for i := 5000; i < 10000; i++ {
		db.Delete(utils.GetTestKey(i))
	}

	stat := db.Stat()
	t.Log(stat)
}
