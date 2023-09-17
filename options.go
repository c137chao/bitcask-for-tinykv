package bitcaskgo

import (
	"bitcask-go/index"
	"os"
	"path/filepath"
)

type Options struct {
	DirPath        string
	Maxsize        int64
	SyncWrite      bool
	SyncThreshHold uint64 // if
	Index          index.IndexType

	MMapAtStartup bool
	MergeRatio    float32
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
	DirPath:        filepath.Join(os.TempDir(), "bitcask-go"),
	Maxsize:        256 * 1024 * 1024,
	SyncWrite:      false,
	SyncThreshHold: 0,
	Index:          index.RBTREE,
	MMapAtStartup:  true,
	MergeRatio:     0.5,
}

var DefaultIterOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 256 * 1024,
	SynWrites:    true,
}
