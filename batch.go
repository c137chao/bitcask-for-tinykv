package bitcaskgo

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTxnSeqno uint64 = 0

var txnFinKey = []byte("txn-fin")

// to support atomic write
// writebatch append a fin record to tag write has been complete
type WriteBatch struct {
	mu            *sync.Mutex
	bitCaskDB     *DB
	options       WriteBatchOptions
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		mu:            new(sync.Mutex),
		bitCaskDB:     db,
		options:       opts,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// put kv to pending write
// it sync to disk file and index when commit
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}

	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// put delete entry to pending write
// if entry with key has been exist in pending but not in datafiledelete, just delete it in pending write
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	pos := wb.bitCaskDB.index.Get(key)

	// if delete un-commit or un-exist logrecord
	if pos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		// else delete a log has never been put ???
		return nil
	}

	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDelete}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// commit pending writes to disk file and index
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > wb.options.MaxBatchSize {
		return ErrExceedMaxBatch
	}

	wb.bitCaskDB.mu.Lock()
	defer wb.bitCaskDB.mu.Unlock()

	// increase sequence nubmer as current transaction number
	txnSeq := atomic.AddUint64(&wb.bitCaskDB.txnSeqNo, 1)

	postions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		log := &data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, txnSeq),
			Value: record.Value,
			Type:  record.Type,
		}

		pos, err := wb.bitCaskDB.appendLogRecord(log)
		if err != nil {
			return err
		}

		postions[string(record.Key)] = pos
	}

	// append a fin logRecord
	commitRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, txnSeq),
		Type: data.LogRecordTxnFin,
	}

	if _, err := wb.bitCaskDB.appendLogRecord(commitRecord); err != nil {
		return err
	}

	// persist
	if wb.options.SynWrites && wb.bitCaskDB.activeFile != nil {
		if err := wb.bitCaskDB.activeFile.Sync(); err != nil {
			return err
		}
	}

	// batch update index
	for _, record := range wb.pendingWrites {
		pos := postions[string(record.Key)]
		wb.bitCaskDB.updateIndex(record.Key, record.Type, pos)
	}

	// clear pendingWrites
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// encode seq+key to bytes
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq)
	copy(encKey[n:], key)

	return encKey
}

// decode bytes to seq+key
func parseLogRecordWithSeq(key []byte) ([]byte, uint64) {
	seqNum, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNum
}
