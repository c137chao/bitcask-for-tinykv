package bitcaskgo

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/sirupsen/logrus"
)

const MergeDir = ".merge"
const MergeFinKey = "merge.Fin"

func (db *DB) getAllOlderFiles() []*data.DataFile {
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	return mergeFiles
}

func (db *DB) checkMergeAvaliable() error {
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		return err
	}
	actualRatio := float32(db.reclaimSize) / float32(totalSize)
	if actualRatio < db.options.MergeRatio {
		logrus.Infof("Merge failed: actual ratio : %v, expected ratio: %v [reclaimSize:%v, totalSize:%v]\n", actualRatio, db.options.MergeRatio, db.reclaimSize, totalSize)
		return nil
	}

	avaliableDiskSize, err := utils.AvaliableDiskSzie()
	if err != nil {
		return err
	}

	if uint64(totalSize-db.reclaimSize) >= avaliableDiskSize {
		return ErrNoEnoughSpaceForMerge
	}

	return nil
}

func (db *DB) tryStartMerging() error {
	if db.isMerging {
		return ErrMergeIsPorgress
	}

	if err := db.checkMergeAvaliable(); err != nil {
		return err
	}
	// start merging
	if err := db.activeFile.Sync(); err != nil {
		return err
	}

	if err := db.setActiveDataFile(); err != nil {
		return err
	}

	return nil
}

//  Merge scan the index
//
//
func (db *DB) Merge() error {
	// return if db is empty
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()

	if err := db.tryStartMerging(); err != nil {
		db.mu.Unlock()
		return err
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	nonMergeFid := db.activeFile.FileId
	mergeFiles := db.getAllOlderFiles()

	db.mu.Unlock()

	// sort merge files by file id
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	logrus.Infof("[Bitcask] Merge from %v to %v", mergeFiles[0].FileId, nonMergeFid-1)

	mergePath := db.getMergePath()

	// remove all old merge files on disk
	if err := os.RemoveAll(mergePath); err != nil {
		return err
	}

	// create new merge  dir
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrite = false

	mergeDB, err := OpenDB(mergeOptions)
	if err != nil {
		return err
	}
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// write all valid record to mergeDN
	// write all index record pos to hint file
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			realkey, _ := parseLogRecordWithSeq(logRecord.Key)
			// TO FIX: index may be change
			// if some item be alterd now, maybe lost it in merge file
			// however, this item musb be persist to datafile after nonMergeFileId
			pos := db.index.Get(realkey)

			// if log record is newest record of key
			if pos != nil && pos.FileId == dataFile.FileId && pos.Offset == offset {
				logRecord.Key = logRecordKeyWithSeq(realkey, nonTxnSeqno)
				// append log record to mergedb active datafile
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// append index logrecord pos to hint file
				if err := hintFile.WriteHintRecord(realkey, pos); err != nil {
					return err
				}

			}

			offset += size
		}
	}

	// sync hint file and merge db
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// write fin file to present merge success
	mergeFinFile, err := data.OpenMergeFinFile(mergePath)
	if err != nil {
		return err
	}

	finRecord := &data.LogRecord{
		Key:   []byte(MergeFinKey),
		Value: []byte(strconv.Itoa(int(nonMergeFid))),
	}
	ecnRecord, _ := data.EncodeLogRecord(finRecord)

	if err := mergeFinFile.Write(ecnRecord); err != nil {
		return err
	}

	if err := mergeFinFile.Sync(); err != nil {
		return err
	}

	return nil
}

// normal: tmp/bitcask
// merge: tmp/bitcaskmerge
func (db *DB) getMergePath() string {
	// path.Clean clean the '/' at end of dirpath
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)

	return filepath.Join(dir, base+MergeDir)
}

// load merge file to datafile path
func (db *DB) loaderMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		logrus.Infof("[Bitcask] Remove Merge Path %v", mergePath)
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// check merge finish
	var mergeFinished bool
	var mergefileNames []string

	for _, ent := range dirEntries {
		if ent.Name() == data.HintFinFileName {
			mergeFinished = true
		}
		if ent.Name() == fileLockName {
			continue
		}
		mergefileNames = append(mergefileNames, ent.Name())
	}

	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		filename := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(filename); err == nil {
			if err := os.Remove(filename); err != nil {
				return err
			}
		}
	}

	// move merge file to data
	for _, fileName := range mergefileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}

	return nil
}

// get file id of first file which hasn't been merging
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	finFile, err := data.OpenMergeFinFile(dirPath)
	if err != nil {
		return 0, err
	}

	record, _, err := finFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFid, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFid), nil
}

// load index from hintfile
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	logrus.Infof("[Bitcask] Open hint file %v", hintFileName)

	var offset int64 = 0
	for {
		record, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos := data.DecodeLogRecordPos(record.Value)
		db.index.Put(record.Key, pos)

		offset += size
	}

	return nil
}
