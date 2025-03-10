package iptree

import (
	"slices"
	"sync"
)

type Element any

type IpOctet[Child Element] struct {
	children [256]*Child
	newChild func() *Child
}

type FirstOctet struct {
	children [4]uint64
}

type FirstOctetAlternative struct {
	sync.Mutex
	children []uint8
}

func (fl *FirstOctetAlternative) addIp(octetVal uint8) uint32 {
	fl.Lock()
	defer fl.Unlock()
	idx := slices.Index(fl.children, octetVal)

	if idx == -1 {
		fl.children = append(fl.children, octetVal)

		return 1
	}

	return 0
}

func (fl *FirstOctet) addIp(octetVal uint8) uint32 {
	var wordIdx int = 0
	var oneOffset int = int(octetVal)
	if octetVal < 64 {
		wordIdx = 0
	} else if octetVal < 128 {
		wordIdx = 1
		oneOffset = oneOffset - 64
	} else if octetVal < 192 {
		wordIdx = 2
		oneOffset = oneOffset - 128
	} else {
		wordIdx = 3
		oneOffset = oneOffset - 192
	}

	var newBit uint64 = 1 << oneOffset

	// child := fl.children[wordIdx]
	// withBit := child | newBit
	fl.children[wordIdx] |= newBit
	// if withBit != child {
	// 	fl.children[wordIdx] = withBit
	// 	return 1
	// }

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
