package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

// mmap io,
type MMap struct {
	readAt *mmap.ReaderAt
}

func NewMMapIOManager(filename string) (*MMap, error) {
	_, err := os.OpenFile(filename, os.O_CREATE, FileDataPerm)
	if err != nil {
		return nil, err
	}

	readAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readAt: readAt}, nil
}

func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readAt.ReadAt(b, offset)
}

func (mmap *MMap) Write([]byte) (int, error) {
	panic("mmap write not implement")
}

func (mmap *MMap) Sync() error {
	return mmap.readAt.Close()
}

func (mmap *MMap) Close() error {
	return mmap.readAt.Close()
}

func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readAt.Len()), nil
}
