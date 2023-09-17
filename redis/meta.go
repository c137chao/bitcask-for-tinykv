package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetaDataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte
	expire   int64
	version  int64
	size     uint32
	head     uint64
	tail     uint64
}

func (md *metadata) encode() []byte {
	var size = maxMetaDataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType

	var index = 1

	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutUvarint(buf[index:], uint64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetaData(buf []byte) *metadata {
	meta := &metadata{}
	meta.dataType = buf[0]
	var index = 1
	var n int

	meta.expire, n = binary.Varint(buf[index:])
	index += n

	meta.version, n = binary.Varint(buf[index:])
	index += n

	size, n := binary.Uvarint(buf[index:])
	index += n

	meta.size = uint32(size)

	if meta.dataType == List {
		meta.head, n = binary.Uvarint(buf[index:])
		index += n

		meta.tail, n = binary.Uvarint(buf[index:])
		index += n
	}

	return meta

}
