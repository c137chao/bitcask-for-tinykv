package bitcaskgo

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

// DB is storage Engine of BitCask
// it supports four opertion for upper application
// Put, Get, Delete, Scan
type DB struct {
	options *Options // config info
	mu      *sync.RWMutex

	// keydir in memory
	// TODO: use a hash index to improve concurrency
	index index.Indexer

	fileIds    []int          // user for build index
	activeFile *data.DataFile // current active file
	olderFiles map[uint32]*data.DataFile
	txnSeq     uint64
	isMerging  bool
}

func OpenDB(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		options:    &options,
		mu:         new(sync.RWMutex),
		index:      index.NewIndexer(options.Index),
		olderFiles: make(map[uint32]*data.DataFile),
	}

	// load merge file to work directory
	if err := db.loaderMergeFiles(); err != nil {
		return nil, err
	}
	// load datafile
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// load index info from hint file
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// create index from data file
	if err := db.loadIndexFromDateFile(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// clase active file
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// close older files
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil

}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

//
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//
	logRecord := data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTxnSeqno),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// append log record to active file, return logRecordPos(fd, offset) of record
	pos, err := db.appendLogRecordWithLock(&logRecord)

	if err != nil {
		return err
	}

	ok := db.index.Put(key, pos)
	if !ok {
		return ErrIndexUpdateFail
	}

	return nil
}

// just append a delete log to datafile
// real data delete will happen in merge
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTxnSeqno),
		Type: data.LogRecordDelete,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFail
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// get <fd, offset> from memory index(keydir)
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPostion(pos)
}

func (db *DB) ListKeys() [][]byte {
	iter := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())

	idx := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
		idx += 1
	}

	return keys
}

func (db *DB) Fold(fn func(key, val []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iter := db.index.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := db.getValueByPostion(iter.Value())
		if err != nil {
			return err
		}
		if !fn(iter.Key(), value) {
			break
		}
	}

	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecord(logRecord)
}

// helper function, appen logrecord to end of active file
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	// check active file, if doesn't exit, create it
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// encode log record and append it to active file
	encRecord, size := data.EncodeLogRecord(logRecord)
	/// if active chunk size is full, create a new active file
	if db.activeFile.WriteOff+size > db.options.Maxsize {
		// persist data fuke to Disk
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// create new active file
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	start := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		FileId: db.activeFile.FileId,
		Offset: start,
	}

	return pos, nil
}

// create a new active datafile
// caller must be hold lock
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0

	if db.activeFile != nil {
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.Maxsize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}

// helper functions, add keyDir item to memory Index
func (db *DB) updateIndex(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
	var ok bool
	if typ == data.LogRecordDelete {
		ok = db.index.Delete(key)
	} else {
		ok = db.index.Put(key, pos)
	}

	if !ok {
		// logrus.Errorf("Delete a un-exist key %s", key)
		// log.Panicf("failed to update key %s index at startup", key)
	}
}

// load datafile from disk
func (db *DB) loadDataFile() error {
	dirEntry, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// iterate dir
	for _, entry := range dirEntry {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			splitFileName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitFileName[0])

			// data file may be corrupted
			if err != nil {
				return ErrDataDirectoryCorrupted
			}

			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)
	db.fileIds = fileIds

	logrus.Infof("Load data File %v", fileIds)

	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// last file is active file
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

//
func (db *DB) loadIndexFromDateFile() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	hasMerge, nonMergeFid := false, uint32(0)
	hintFinFileName := filepath.Join(db.options.DirPath, data.HintFinFileName)

	if _, err := os.Stat(hintFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFid = fid
	}

	// cache tranction operation
	txnRecords := make(map[uint64][]*data.TransactionRecord)
	curSeqNo := nonTxnSeqno

	// lood must be order by file Id due to log structured
	for i, fid := range db.fileIds {
		var fileid = uint32(fid)

		// skip file has been merged
		if hasMerge && fileid < nonMergeFid {
			continue
		}

		var dataFile *data.DataFile
		if fileid == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileid]
		}
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// insert keydir entry to index
			pos := &data.LogRecordPos{FileId: fileid, Offset: offset}
			realKey, seqNo := parseLogRecordWithSeq(logRecord.Key)

			// Not write batch
			if seqNo == nonTxnSeqno {
				db.updateIndex(realKey, logRecord.Type, pos)
			} else {
				if logRecord.Type == data.LogRecordTxnFin {
					for _, batchRecord := range txnRecords[seqNo] {
						db.updateIndex(batchRecord.Record.Key, batchRecord.Record.Type, batchRecord.Pos)
					}
					delete(txnRecords, seqNo)
				} else {
					// if not found commit flag, cache it to txn
					logRecord.Key = realKey
					txnRecord := &data.TransactionRecord{
						Record: logRecord,
						Pos:    pos,
					}
					txnRecords[seqNo] = append(txnRecords[seqNo], txnRecord)
				}
			}

			if seqNo > curSeqNo {
				curSeqNo = seqNo
			}

			offset += size

			if i == len(db.fileIds)-1 {
				db.activeFile.WriteOff = offset
			}
		}
	}

	db.txnSeq = curSeqNo

	return nil
}

func (db *DB) getValueByPostion(pos *data.LogRecordPos) ([]byte, error) {
	datafile := db.activeFile
	if pos.FileId != datafile.FileId {
		datafile = db.olderFiles[pos.FileId]
	}

	if datafile == nil {
		return nil, ErrDataFileNotFound
	}

	logrecord, _, err := datafile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if logrecord.Type != data.LogRecordNormal {
		return nil, ErrKeyNotFound
	}

	return logrecord.Value, nil
}
