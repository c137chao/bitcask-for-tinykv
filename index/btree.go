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

type BTreeItem struct {
	key []byte
	val *data.LogRecordPos
}

func (it BTreeItem) Less(than btree.Item) bool {
	return bytes.Compare(it.key, than.(*BTreeItem).key) == -1
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

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) IndexValueType {
	it := &BTreeItem{key: key, val: pos}
	bt.mu.Lock()
	defer bt.mu.Unlock()
	oldItem := bt.tree.ReplaceOrInsert(it)

	if oldItem == nil {
		return nil
	}
	return oldItem.(*BTreeItem).val
}

func (bt *BTree) Get(key []byte) IndexValueType {
	it := &BTreeItem{key: key}
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	getResult := bt.tree.Get(it)
	if getResult == nil {
		return nil
	}
	return getResult.(*BTreeItem).val
}

func (bt *BTree) Delete(key []byte) IndexValueType {
	it := &BTreeItem{key: key}
	bt.mu.Lock()
	defer bt.mu.Unlock()

	deleteItem := bt.tree.Delete(it)
	if deleteItem == nil {
		return nil
	}

	return deleteItem.(*BTreeItem).val
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
	values    []*BTreeItem
}

func newBtreeIter(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*BTreeItem, tree.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*BTreeItem)
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
