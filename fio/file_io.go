package fio

import "os"

// implement IOManager
// FileIO just encapsulation go stand file io operation
type FileIO struct {
	file *os.File
}

const FileDataPerm = 0644

func NewFileIOManager(filename string) (*FileIO, error) {
	file, err := os.OpenFile(
		filename,
		os.O_APPEND|os.O_CREATE|os.O_RDWR,
		FileDataPerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{file: file}, nil
}

func (fio *FileIO) Read(data []byte, off int64) (int, error) {
	return fio.file.ReadAt(data, off)
}

func (fio *FileIO) Write(data []byte) (int, error) {
	return fio.file.Write(data)
}

func (fio *FileIO) Sync() error {
	return fio.file.Sync()
}

func (fio *FileIO) Close() error {
	return fio.file.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
