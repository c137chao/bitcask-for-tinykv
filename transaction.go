package bitcaskgo

const (
	Active = iota
	Commit
	Abort
)

type Txn struct {
	id     uint64
	db     *DB
	status interface{}
}
