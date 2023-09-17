package index

import (
	"sync"

	goapt "github.com/plar/go-adaptive-radix-tree"
)

// TODO: complete art index methord

type AdaptiveRadixTree struct {
	mu   *sync.RWMutex
	tree goapt.Tree
}

func NewARTIndex() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		mu:   new(sync.RWMutex),
		tree: goapt.New(),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, value IndexValueType) IndexValueType {
	panic("unimplement adaptive radix tree index")
}

func (art *AdaptiveRadixTree) Get(key []byte) IndexValueType {
	panic("unimplement adaptive radix tree index")

}

func (art *AdaptiveRadixTree) Delete(key []byte) IndexValueType {
	panic("unimplement adaptive radix tree index")

}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	panic("unimplement adaptive radix tree index")

}

func (art *AdaptiveRadixTree) Size() int {
	panic("unimplement adaptive radix tree index")

}
