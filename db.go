package bitcaskgo

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
	"github.com/sirupsen/logrus"
)

const fileLockName = "flock"

// DB is storage Engine of BitCask
// it supports four opertion for upper application
// Put, Get, Delete, Scan
type DB struct {
	mu       *sync.RWMutex
	filelock *flock.Flock // only on process can use DB

	options Options // config options

	// keydir in memory
	// TODO: use a hash index to improve concurrency
	index      index.Indexer
	activeFile *data.DataFile // current active file
	olderFiles map[uint32]*data.DataFile

	txnSeqNo  uint64 // used for write bantch
	isInitial bool   // used for
	isMerging bool   //
	fileIds   []int  // user for build index

	bytesWrite  uint64 // bytes has been write
	reclaimSize int64  // unvalid bytes has been write
}

type Stat struct {
	KeyNum      uint
	DataFileNum uint
	ReclaimSize int64
	DiskSize    int64
}

// return statistic info about db
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	return &Stat{
		KeyNum:      uint(db.index.Size()),
		DataFileNum: dataFiles,
		ReclaimSize: db.reclaimSize,
		DiskSize:    0,
	}

}

// open bitcask db
func OpenDB(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	filelock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := filelock.TryLock()
	if err != nil {
		return nil, err
	}

	if !hold {
		return nil, ErrDataBaseIsUsing
	}

	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		index:      index.NewIndexer(options.Index),
		olderFiles: make(map[uint32]*data.DataFile),
		isInitial:  isInitial,
		filelock:   filelock,
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

	if db.options.MMapAtStartup {
		if err := db.resetIoType(); err != nil {
			return nil, err
		}
	}

	logrus.Infof("[Bitcask] OpenDB at %v, active fid: %v, total entries: %v\n",
		options.DirPath, db.activeFile.FileId, db.index.Size())

	return db, nil
}

// close bitcask db
// free file lock and close all files
func (db *DB) Close() error {
	defer func() {
		if err := db.filelock.Unlock(); err != nil {
			logrus.Fatalf("err %v, failed to unlock the dir %v", err, db.options.DirPath)
		}
	}()

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

// sync file to disk
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

func (db *DB) Backup(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	logrus.Infof("[bitcask] generate a backup to dir %v\n", dir)
	return utils.Copy(db.options.DirPath, dir, []string{fileLockName})
}

// Append <key, value> to active file
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

	logrus.Debugf("[bitcask] %v put <%s, %s>, position <fid:%v, off:%v>\n", db.options.DirPath, key, value, pos.FileId, pos.Offset)
	if oldpos := db.index.Put(key, pos); oldpos != nil {
		db.reclaimSize += int64(oldpos.Size)
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

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	db.reclaimSize += int64(pos.Size)
	deletePos := db.index.Delete(key)
	if deletePos == nil {
		return ErrIndexUpdateFail
	}

	logrus.Debugf("[bitcask] %v delete <%s>, position <fid:%v, off:%v>\n", db.options.DirPath, key, pos.FileId, pos.Offset)

	db.reclaimSize += int64(deletePos.Size)

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
	logrus.Debugf("[bitcask] %v get <%s>, position <fid:%v, off:%v>\n", db.options.DirPath, key, pos.FileId, pos.Offset)

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

	db.bytesWrite += uint64(size)

	if (db.options.SyncThreshHold != 0 && db.bytesWrite >= db.options.SyncThreshHold) || db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.bytesWrite = 0
	}

	pos := &data.LogRecordPos{
		FileId: db.activeFile.FileId,
		Offset: start,
		Size:   uint32(size),
	}

	return pos, nil
}

// create a new active datafile
// caller must be hold lock
func (db *DB) setActiveDataFile() error {
	var activeFileId uint32 = 0

	if db.activeFile != nil {
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		activeFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, activeFileId, fio.StandFileIO)
	if err != nil {
		return err
	}
	logrus.Debugf("[bitcask] set active data file %v\n", activeFileId)
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
	if options.MergeRatio <= 0 || options.MergeRatio >= 1 {
		return errors.New("unvalid merge ration which should 0 < mergeratio < 1")
	}

	return nil
}

// helper functions, add keyDir item to memory Index
func (db *DB) updateIndex(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
	var oldPos *data.LogRecordPos
	if typ == data.LogRecordDelete {
		oldPos = db.index.Delete(key)
		db.reclaimSize += int64(pos.Size)
	} else {
		oldPos = db.index.Put(key, pos)
	}

	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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

	for i, fid := range fileIds {
		iotyp := fio.StandFileIO
		if db.options.MMapAtStartup {
			iotyp = fio.MemoryMapIO
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), iotyp)
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

	nonMergeFid := uint32(0)
	hintFinFileName := filepath.Join(db.options.DirPath, data.HintFinFileName)

	if _, err := os.Stat(hintFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		nonMergeFid = fid
	}

	// cache tranction operation
	txnRecords := make(map[uint64][]*data.TransactionRecord)
	curSeqNo := nonTxnSeqno

	// lood must be order by file Id due to log structured
	for i, fid := range db.fileIds {
		var fileid = uint32(fid)

		// skip file has been merged
		if fileid < nonMergeFid {
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
			if err == io.EOF {
				break
			}
			if err != nil { // err not nil and not eof
				return err
			}

			// insert keydir entry to index
			pos := &data.LogRecordPos{FileId: fileid, Offset: offset, Size: uint32(size)}
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
		}

		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	db.txnSeqNo = curSeqNo

	return nil
}

func (db *DB) getValueByPostion(pos *data.LogRecordPos) ([]byte, error) {
	logrus.Infof("get value from file %v, offset %v\n", pos.FileId, pos.Offset)
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

func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.SetIoMananger(db.options.DirPath, fio.StandFileIO); err != nil {
		return err
	}

	for _, datafile := range db.olderFiles {
		if err := datafile.SetIoMananger(db.options.DirPath, fio.StandFileIO); err != nil {
			return err
		}
	}

	return nil
}
