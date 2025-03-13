package iptree

import (
	"sync"
)

type Element any

type IpOctet[Child Element] struct {
	sync.RWMutex
	children [256]*Child
	newChild func() *Child
}

type FirstOctet struct {
	sync.Mutex
	bitmap [4]uint64
}

const (
	upTo63Mask  = uint8(0b00111111)
	getIdxShift = uint8(6)
)

func octetsOffsetAndIdx(octetVal uint8) (idx int, newBit uint64) {
	idx = int(octetVal) >> getIdxShift // lets us know 2 upper bits
	offset := upTo63Mask & octetVal    // let's us use 6 lower bits (2^6 = 64 which makes sense since we use uint64)
	newBit = 1 << offset

	return idx, newBit
}

func (fl *FirstOctet) addIp(octetVal uint8) uint32 {
	idx, newBit := octetsOffsetAndIdx(octetVal)

	fl.Lock()
	defer fl.Unlock()
	return fl.addBitOptimistic(idx, newBit)
}

func (fl *FirstOctet) addBitOptimistic(idx int, newBit uint64) uint32 {
	bitmapSection := fl.bitmap[idx]
	withBit := bitmapSection | newBit

	fl.bitmap[idx] = withBit
	if withBit != bitmapSection {
		return 1
	}

	return 0
}

type SecondLevel = IpOctet[FirstOctet]
type ThirdLevel = IpOctet[SecondLevel]
type RootLevel = IpOctet[ThirdLevel]

func SecondsChild() *FirstOctet {
	return &FirstOctet{}
}

func ThirdsChild() *SecondLevel {
	return &SecondLevel{
		newChild: SecondsChild,
	}
}

func FourthsChild() *ThirdLevel {
	return &ThirdLevel{
		newChild: ThirdsChild,
	}
}

func (lvl *IpOctet[Child]) GetChild(part uint8) *Child {
	lvl.RLock()
	element := lvl.children[part]
	lvl.RUnlock()
	if element != nil {
		return element
	}

	lvl.Lock()
	defer lvl.Unlock()

	element = lvl.children[part]
	if element != nil {
		return element
	}
	element = lvl.newChild()
	lvl.children[part] = element

	return element
}

func (lvl *IpOctet[Child]) GetChildOptimistic(part uint8) *Child {
	element := lvl.children[part]
	if element != nil {
		return element
	}
	element = lvl.newChild()
	lvl.children[part] = element

	return element
}

func (lvl *IpOctet[Child]) Populate() {
	for i := 0; i < 256; i++ {
		lvl.children[i] = lvl.newChild()
	}
}
