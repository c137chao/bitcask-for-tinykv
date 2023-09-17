package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

// crc: 4byte, type: 1 byte, keysize and value size: 5(2^5 = 32)
const LogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
	LogRecordTxnFin
)

// log record header
// crc + type + keysize + valuesize
// keysize and value size store to disk as varint
type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valSize    uint32
}

// log record content
// key + value + type
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// keydir value in memory
type LogRecordPos struct {
	FileId uint32
	Size   uint32
	Offset int64
}

//
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// encode logrecord to binary bytes,
// return bytes and size
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, LogRecordHeaderSize)
	var crcSize = 4
	var index = crcSize

	header[index] = logRecord.Type
	index += 1

	// use varint to save storage space
	index += binary.PutUvarint(header[index:], uint64(len(logRecord.Key)))
	index += binary.PutUvarint(header[index:], uint64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encRecord := make([]byte, size)

	// copy header to encode Record
	copy(encRecord[:index], header[:index])

	// copy key and value to encode Record
	index += copy(encRecord[index:], logRecord.Key)
	index += copy(encRecord[index:], logRecord.Value)

	// caculate crc checking code
	crc := crc32.ChecksumIEEE(encRecord[crcSize:])
	binary.LittleEndian.PutUint32(encRecord[:crcSize], crc)

	return encRecord, int64(size)
}

// decode binary bytes to logRecord header
// return headerf and header size
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	index := 5
	keySize, n := binary.Uvarint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valSize, m := binary.Uvarint(buf[index:])
	header.valSize = uint32(valSize)
	index += m

	return header, int64(index)
}

// caculate crc checking code of logrecord
func getRecordCRC(log *LogRecord, header []byte) uint32 {
	if log == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[4:])
	crc = crc32.Update(crc, crc32.IEEETable, log.Key)
	crc = crc32.Update(crc, crc32.IEEETable, log.Value)

	return crc
}

// encord logrecordPos to bytes
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutUvarint(buf[index:], uint64(pos.FileId))
	index += binary.PutUvarint(buf[index:], uint64(pos.Size))
	index += binary.PutVarint(buf[index:], pos.Offset)

	return buf[:index]
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Uvarint(buf[index:])
	index += n
	size, _ := binary.Uvarint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])

	return &LogRecordPos{
		FileId: uint32(fileId),
		Size:   uint32(size),
		Offset: offset,
	}
}
