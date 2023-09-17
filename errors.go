package bitcaskgo

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFail        = errors.New("fail to update index")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("the database dir may be corrupted")
	ErrExceedMaxBatch         = errors.New("exceed the max batch size")
	ErrMergeIsPorgress        = errors.New("merge is in progres, try merge later")
	ErrDataBaseIsUsing        = errors.New("other porcess is using data base")
	ErrMergeRationUnreached   = errors.New("the merge ration has not reach threshold")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough disl space for merge")
)
