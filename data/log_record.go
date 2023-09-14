package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

// -----------+----------+--------------+-----------------
// |    CRC   |   Type   |   Key size   |   Value Size   |
// -----------+----------+--------------+-----------------
//
// crc: 4byte, type: 1 byte, keysize and value size: 5(2^5 = 32)
const LogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
	LogRecordTxnFin
)

type LogRecordHeader struct {
	Crc        uint32
	RecordType LogRecordType
	KeySize    uint32
	ValSize    uint32
}

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// data struce of keydir in memory
type LogRecordPos struct {
	FileId uint32
	Offset int64
}

//
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

//
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
	copy(encRecord[index:], logRecord.Key)
	copy(encRecord[index+len(logRecord.Key):], logRecord.Value)

	// caculate crc checking code
	crc := crc32.ChecksumIEEE(encRecord[crcSize:])
	binary.LittleEndian.PutUint32(encRecord[:crcSize], crc)

	return encRecord, int64(size)
}

func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		Crc:        binary.LittleEndian.Uint32(buf[:4]),
		RecordType: buf[4],
	}
	index := 5
	keySize, n := binary.Uvarint(buf[index:])
	header.KeySize = uint32(keySize)
	index += n

	valSize, m := binary.Uvarint(buf[index:])
	header.ValSize = uint32(valSize)
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

func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.FileId))
	index += binary.PutVarint(buf[index:], pos.Offset)

	return buf[:index]
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])

	return &LogRecordPos{
		FileId: uint32(fileId),
		Offset: offset,
	}
}
