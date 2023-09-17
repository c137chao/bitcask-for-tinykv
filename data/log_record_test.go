package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	log := &LogRecord{
		Key:   []byte("default-key"),
		Value: []byte("default-value"),
		Type:  LogRecordNormal,
	}

	encLog, size := EncodeLogRecord(log)
	assert.NotNil(t, encLog)
	assert.Greater(t, size, int64(5))

	t.Logf("encode version: %v, len %v", encLog, size)

}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := DecodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(8), h1.keySize)
	assert.Equal(t, uint32(20), h1.valSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := DecodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(8), h2.keySize)
	assert.Equal(t, uint32(0), h2.valSize)

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := DecodeLogRecordHeader(headerBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, LogRecordDelete, h3.recordType)
	assert.Equal(t, uint32(8), h3.keySize)
	assert.Equal(t, uint32(20), h3.valSize)
}
