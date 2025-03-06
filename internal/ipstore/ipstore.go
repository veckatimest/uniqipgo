package ipstore

import "fmt"

type bitmap [4]uint64

var zeroBitmap = bitmap{0, 0, 0, 0}

func (b bitmap) String() string {
	return fmt.Sprintf("bitmap[%x %x %x %x]", b[0], b[1], b[2], b[3])
}

func (bm bitmap) has(mask bitmap) bool {
	for i, b := range bm {
		if mask[i]&b != 0 {
			return true
		}
	}
	return false
}

func (bm bitmap) apply(mask bitmap) bitmap {
	ret := zeroBitmap
	for i, b := range bm {
		ret[i] = mask[i] | b
	}
	return ret
}

type IPStore struct {
	buckets []bitmap
	count   int
}

const nBuckets = 0xff_ff_ff

func NewIPStore() *IPStore {
	buckets := make([]bitmap, nBuckets)
	for i := 0; i < nBuckets; i++ {
		buckets = append(buckets, zeroBitmap)
	}
	return &IPStore{
		buckets: buckets,
		count:   0,
	}
}

func (s *IPStore) Insert(ip uint32) {
	hash := ipStoreHash(ip)
	mask := ipToBitmap(ip)
	if s.buckets[hash].has(mask) {
		return
	}
	s.buckets[hash] = s.buckets[hash].apply(mask)
	s.count++
}

func (s *IPStore) Count() int {
	return s.count
}

func ipStoreHash(ip uint32) int {
	return int((ip & 0xff_ff_ff_00) >> 8)
}

func ipToBitmap(ip uint32) bitmap {
	b := ip & 0xff

	if b < 64 {
		return bitmap{1 << b, 0, 0, 0}
	} else if b < 128 {
		return bitmap{0, 1 << (b % 64), 0, 0}
	} else if b < 192 {
		return bitmap{0, 0, 1 << (b % 128), 0}
	} else {
		return bitmap{0, 0, 0, 1 << (b % 192)}
	}
}
