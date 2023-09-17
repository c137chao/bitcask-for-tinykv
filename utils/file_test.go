package utils

import (
	"bitcask-go/fio"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	size, err := DirSize(dir)
	t.Log(size)
	t.Log(err)
}

func TestAvaliableDiskSize(t *testing.T) {
	size, err := AvaliableDiskSzie()
	assert.Nil(t, err)

	t.Logf("%v GB\n", size/1024/1024/1024)
}

func TestCopyDir(t *testing.T) {
	src, _ := os.MkdirTemp("", "bitcask-go-src")
	dstPath := filepath.Join(filepath.Dir(src), "bitcask-go-dst")

	file1, _ := os.OpenFile(filepath.Join(src, "demo1"), os.O_CREATE|os.O_RDWR, fio.FileDataPerm)
	file1.Write([]byte("some contents any thing"))
	file2, _ := os.OpenFile(filepath.Join(src, "demo2"), os.O_CREATE|os.O_RDWR, fio.FileDataPerm)
	file2.Write([]byte("sxsad sdasdas sadasda asdasd "))
	file1.Sync()
	file2.Sync()

	Copy(src, dstPath, []string{})
}
