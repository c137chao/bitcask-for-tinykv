package fio

type FileIOType = byte

const (
	StandFileIO FileIOType = iota
	MemoryMapIO
)

type IOManager interface {
	// read data at offset from file to byte array, return size of read
	Read([]byte, int64) (int, error)

	// write n size froem []byte to file
	Write([]byte) (int, error)

	// Sync file to disk
	Sync() error

	// close file
	Close() error

	// file size
	Size() (int64, error)
}

//
func NewIOManager(filename string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandFileIO:
		return NewFileIOManager(filename)
	case MemoryMapIO:
		return NewMMapIOManager(filename)
	default:
		panic("unsupport io type")
	}
}
