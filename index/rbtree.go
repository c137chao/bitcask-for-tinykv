package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	"github.com/HuKeping/rbtree"
	"github.com/sirupsen/logrus"
)

type RBTree struct {
	mu   *sync.RWMutex
	tree *rbtree.Rbtree
}

type RBTreeItem struct {
	key []byte
	val *data.LogRecordPos
}

func (it RBTreeItem) Less(than rbtree.Item) bool {
	return bytes.Compare(it.key, than.(*RBTreeItem).key) == -1
}

func NewRBTree() *RBTree {
	return &RBTree{
		mu:   new(sync.RWMutex),
		tree: rbtree.New(),
	}
}

func (rbt *RBTree) Put(key []byte, value IndexValueType) IndexValueType {
	it := &RBTreeItem{key: key, val: value}

	rbt.mu.Lock()
	defer rbt.mu.Unlock()
	oldItem := rbt.tree.InsertOrGet(it).(*RBTreeItem)

	if oldItem.val == value {
		return nil
	}

	oldval := oldItem.val
	oldItem.val = value

	return oldval
}

func (rbt *RBTree) Get(key []byte) IndexValueType {
	it := &RBTreeItem{key: key}

	rbt.mu.RLock()
	defer rbt.mu.RUnlock()

	item := rbt.tree.Get(it)
	if item == nil {
		return nil
	}
	return item.(*RBTreeItem).val
}

func (rbt *RBTree) Delete(key []byte) IndexValueType {
	it := &RBTreeItem{key: key}
	rbt.mu.Lock()
	defer rbt.mu.Unlock()

	item := rbt.tree.Delete(it)
	if item == nil {
		return nil
	}
	return item.(*RBTreeItem).val
}

func (rbt *RBTree) Iterator(reverse bool) Iterator {
	if rbt.tree == nil {
		return nil
	}
	rbt.mu.Lock()
	defer rbt.mu.Unlock()

	return newRBtreeIter(rbt.tree, reverse)
}

func (rbt *RBTree) Size() int {
	rbt.mu.RLock()
	defer rbt.mu.RUnlock()
	return int(rbt.tree.Len())
}

// BTree iter
type rbtreeIterator struct {
	currIndex int //
	reverse   bool
	values    []*RBTreeItem
}

func newRBtreeIter(tree *rbtree.Rbtree, reverse bool) *rbtreeIterator {
	var idx int
	values := make([]*RBTreeItem, tree.Len())

	saveValues := func(it rbtree.Item) bool {
		item, ok := it.(*RBTreeItem)
		if !ok {
			logrus.Infof("rbtree iterm instance failed")
			return false
		}

		values[idx] = item
		idx++
		return true
	}

	if reverse {
		tree.Descend(tree.Max(), saveValues)
	} else {
		tree.Ascend(tree.Min(), saveValues)
	}

	return &rbtreeIterator{currIndex: 0, values: values, reverse: reverse}
}

func (iter *rbtreeIterator) Rewind() {
	iter.currIndex = 0
}

func (iter *rbtreeIterator) Seek(key []byte) {
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

func (iter *rbtreeIterator) Next() {
	iter.currIndex += 1
}

func (iter *rbtreeIterator) Valid() bool {
	return iter.currIndex < len(iter.values)
}

func (iter *rbtreeIterator) Key() []byte {
	return iter.values[iter.currIndex].key
}

func (iter *rbtreeIterator) Value() *data.LogRecordPos {
	return iter.values[iter.currIndex].val
}

func (iter *rbtreeIterator) Close() {
	iter.values = nil
}
