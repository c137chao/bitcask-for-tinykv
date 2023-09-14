package index

import (
	"bitcask-go/data"
	"bytes"
	"sync"

	"github.com/HuKeping/rbtree"
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

func (rbt *RBTree) Put(key []byte, value *data.LogRecordPos) bool {
	rbt.mu.Lock()
	defer rbt.mu.Unlock()
	rbt.tree.Insert(RBTreeItem{key: key, val: value})
	return true
}

func (rbt *RBTree) Get(key []byte) *data.LogRecordPos {
	rbt.mu.RLock()
	defer rbt.mu.RUnlock()
	item := rbt.tree.Get(RBTreeItem{key: key})
	if item == nil {
		return nil
	}
	return item.(*RBTreeItem).val
}

func (rbt *RBTree) Delete(key []byte) bool {
	rbt.mu.Lock()
	defer rbt.mu.Unlock()

	item := rbt.tree.Delete(RBTreeItem{key: key}).(*RBTreeItem)
	return item != nil && item.val != nil
}

func (rbt *RBTree) Iterator(reverse bool) Iterator {
	panic("not implement")
}

func (rbt *RBTree) Size() int {
	rbt.mu.RLock()
	defer rbt.mu.RUnlock()
	return int(rbt.tree.Len())
}
