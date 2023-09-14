package bitcaskgo

import (
	"bitcask-go/index"
	"os"
)

type Options struct {
	DirPath   string
	Maxsize   int64
	SyncWrite bool
	Index     index.IndexType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type WriteBatchOptions struct {
	MaxBatchSize int
	SynWrites    bool
}

var DefaultOptions = Options{
	DirPath:   os.TempDir(),
	Maxsize:   256 * 1024 * 1024,
	SyncWrite: false,
	Index:     index.BTREE,
}

var DefaultIterOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 256 * 1024,
	SynWrites:    true,
}
