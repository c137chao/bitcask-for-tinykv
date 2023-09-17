package fio

import (
	"log"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Basic(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-basic.data")
	defer destroyFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	b := make([]byte, 100)
	n, err := mmapIO.Read(b, 0)

	log.Printf("n: %v", n)
	log.Printf("err: %v", err)

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)

	fio.Write([]byte("hello Mr Zhuo"))
	fio.Sync()

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	n, err = mmapIO2.Read(b, 0)

	log.Printf("s: %s", b)
	log.Printf("n: %v", n)
	log.Printf("err: %v", err)

}
