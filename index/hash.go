package index

import "sync"

type HashIndex struct {
	mu   *sync.RWMutex
	hash map[string]IndexValueType
}

func (h *HashIndex) Put(key []byte, value IndexValueType) IndexValueType {
	h.mu.Lock()
	defer h.mu.Unlock()

	oldval := h.hash[string(key)]
	h.hash[string(key)] = value

	return oldval
}

// get value with key, value is rid of data, return nil if key doesn't exist
func (h *HashIndex) Get(key []byte) IndexValueType {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.hash[string(key)]
}

// delete item with key, return old value if delete successm, nil if key doesn't exist
func (h *HashIndex) Delete(key []byte) IndexValueType {
	h.mu.Lock()
	defer h.mu.Unlock()

	if oldval, ok := h.hash[string(key)]; ok {
		delete(h.hash, string(key))
		return oldval
	}

	return nil
}

// create a iterator for index
func (h *HashIndex) Iterator(reverse bool) Iterator {
	panic("Unimplement")
}

// return item count of index
func (h *HashIndex) Size() int {
	return len(h.hash)
}
