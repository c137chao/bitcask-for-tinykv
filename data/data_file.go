package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

const (
	DataFileSuffix  = ".data"
	HintFileName    = "hint-index"
	HintFinFileName = "hint-fin"
)

var ErrInvalidCRC = errors.New("invalid crc checking code")

// ---- bitcask data file in disk ----
//
// datafile consists of logs (LogRecordHeader + logRecord),
// IOManager abstract io operation, it can be file io or mmap
// datafile name is format as xxxx.data, xxxx is file id
//
type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

// open or create file in dirpath with fid,
//  full path is format as /dirpath/xxx.data, xxx is fid
func OpenDataFile(dirPath string, fid uint32, iotype fio.FileIOType) (*DataFile, error) {
	filename := filepath.Join(dirPath, fmt.Sprintf("%09d", fid)+DataFileSuffix)
	return newDataFile(filename, fid, iotype)
}

// Open Hint file, Hint file is consist of all records in index
func OpenHintFile(dirPath string) (*DataFile, error) {
	filename := filepath.Join(dirPath, HintFileName)
	return newDataFile(filename, 0, fio.StandFileIO)
}

// Merge Finish File exist present merging complete
func OpenMergeFinFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFinFileName)
	return newDataFile(fileName, 0, fio.StandFileIO)
}

func newDataFile(fileName string, fid uint32, iotype fio.FileIOType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName, iotype)
	size, _ := ioManager.Size()
	if err != nil {
		return nil, err
	}

	return &DataFile{FileId: fid, WriteOff: size, IoManager: ioManager}, nil
}

// format filename as dirPath/fid.data
//  example "tmp/bitcast-go/000000001.data"
func GetDataFileName(dirPath string, fid uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fid)+DataFileSuffix)
}

// format binary sequence using Item {key, position}, and write it to IO stream
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	hintRecord := &LogRecord{
		Key:   key,                     // key is already binary seq
		Value: EncodeLogRecordPos(pos), // encode position as binary sequence
	}

	encRecord, _ := EncodeLogRecord(hintRecord)

	return df.Write(encRecord)
}

// read logRecord from disk datafile at offset
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = LogRecordHeaderSize

	// check some special case
	// in some case, header size + kv size maybe less than maxHeaderSize
	if headerBytes+offset > fileSize {
		headerBytes = fileSize - offset
	}

	// reader log header
	headerBuf, err := df.ReadNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := DecodeLogRecordHeader(headerBuf)
	headerBuf = headerBuf[:headerSize]

	// return error EOF if header is empty or no logEntry at offset
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valSize == 0 {
		return nil, 0, io.EOF
	}

	// read Log record
	var kvSize int64 = int64(header.keySize + header.valSize)
	logRecordSize := headerSize + kvSize
	logRecord := &LogRecord{Type: header.recordType}

	if kvSize > 0 {
		kvBuf, err := df.ReadNBytes(kvSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:header.keySize]
		logRecord.Value = kvBuf[header.keySize:]
	}

	// check CRC
	crc := getRecordCRC(logRecord, headerBuf)
	if crc != header.crc {
		logrus.Errorf("crc checking code doesn't match at file %v offset %v", df.FileId, offset)
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, int64(logRecordSize), nil
}

func (df *DataFile) Read(buffer []byte, offset int64) (int, error) {
	return df.IoManager.Read(buffer, offset)
}

func (df *DataFile) Write(data []byte) error {
	n, err := df.IoManager.Write(data)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return err
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) Size() (int64, error) {
	return df.IoManager.Size()
}

func (df *DataFile) ReadNBytes(n int64, offset int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := df.IoManager.Read(buf, offset)
	return buf, err
}

// close old ioManager and create new IoManger with specified IO type
func (df *DataFile) SetIoMananger(dirPath string, iotype fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), iotype)
	if err != nil {
		return err
	}

	df.IoManager = ioManager
	return nil
}
