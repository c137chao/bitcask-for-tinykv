package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

const default_degree int = 32

// BTree is encapsulation of google btree
// you can see more at https://pkg.go.dev/github.com/google/btree
type BTree struct {
	tree *btree.BTree
	mu   *sync.RWMutex
}

// create new a btree
func newBTree(degree int) *BTree {
	if degree < 2 {
		degree = default_degree
	}
	return &BTree{
		tree: btree.New(degree),
		mu:   new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, val: pos}
	bt.mu.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.mu.Unlock()

	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	getResult := bt.tree.Get(it)
	if getResult == nil {
		return nil
	}
	return getResult.(*Item).val
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.mu.Lock()
	defer bt.mu.Unlock()

	oldItem := bt.tree.Delete(it)

	return oldItem != nil
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}

	bt.mu.RLock()
	defer bt.mu.RUnlock()

	return newBtreeIter(bt.tree, reverse)
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// BTree iter
type btreeIterator struct {
	currIndex int //
	reverse   bool
	values    []*Item
}

func newBtreeIter(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{currIndex: 0, values: values, reverse: reverse}
}

func (iter *btreeIterator) Rewind() {
	iter.currIndex = 0
}

func (iter *btreeIterator) Seek(key []byte) {
	if iter.reverse {
		iter.currIndex = sort.Search(len(iter.values), func(i int) bool {
			return bytes.Compare(iter.values[i].key, key) <= 0
		})
	} else {
		iter.currIndex = sort.Search(len(iter.values), func(i int) bool {
			return bytes.Compare(iter.values[i].key, key) >= 0
		})
	}
}

func (iter *btreeIterator) Next() {
	iter.currIndex += 1
}

func (iter *btreeIterator) Valid() bool {
	return iter.currIndex < len(iter.values)
}

func (iter *btreeIterator) Key() []byte {
	return iter.values[iter.currIndex].key
}

func (iter *btreeIterator) Value() *data.LogRecordPos {
	return iter.values[iter.currIndex].val
}

func (iter *btreeIterator) Close() {
	iter.values = nil
}
