package iptree

import (
	"math/bits"
	"sync"
)

type Merger[T any] interface {
	MergeWith(other T)
}

type Element any

type LevelChild[Child Element] interface {
	AddElement([]uint8) bool
	GetChild(uint8) Child
}

type BitCounter interface {
	CountBits() uint32
}

type IpLevel[Child Element] struct {
	sync.RWMutex
	children [256]*Child
	newChild func() *Child
}

type FirstLevel struct {
	sync.Mutex
	children [4]uint64
}

func (fl *FirstLevel) CountBits() uint32 {
	sum := bits.OnesCount64(fl.children[0]) +
		bits.OnesCount64(fl.children[1]) +
		bits.OnesCount64(fl.children[2]) +
		bits.OnesCount64(fl.children[3])

	return uint32(sum)
}

func (fl *FirstLevel) MergeWith(other *FirstLevel) {
	for i := 0; i < 4; i++ {
		fl.children[i] |= other.children[i]
	}
}

func (fl *FirstLevel) addBit(part uint8) bool {
	var wordIdx int = 0
	var oneOffset int = int(part)
	if part < 64 {
		wordIdx = 0
	} else if part < 128 {
		wordIdx = 1
		oneOffset = oneOffset - 64
	} else if part < 192 {
		wordIdx = 2
		oneOffset = oneOffset - 128
	} else {
		wordIdx = 3
		oneOffset = oneOffset - 192
	}

	var newBit uint64 = 1 << oneOffset

	fl.Lock()
	defer fl.Unlock()
	child := fl.children[wordIdx]
	withBit := child | newBit

	if withBit != child {
		fl.children[wordIdx] = withBit
		return true
	}

	return false
}

type SecondLevel = IpLevel[FirstLevel]
type ThirdLevel = IpLevel[SecondLevel]
type FourthLevel = IpLevel[ThirdLevel]

func NewRoot() *FourthLevel {
	var rootChildren [256]*ThirdLevel

	var wg sync.WaitGroup
	for i := 0; i < 256; i++ {
		newChild := FourthsChild()
		rootChildren[i] = newChild
		go newChild.Populate(&wg)
		wg.Add(1)
	}

	wg.Wait()

	return &FourthLevel{
		newChild: FourthsChild,
		children: rootChildren,
	}
}

func SecondsChild() *FirstLevel {
	return &FirstLevel{}
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

func (lvl *IpLevel[Child]) MergeWith(other *IpLevel[Child]) {
	for i, child := range lvl.children {
		otherChild := other.children[i]

		if otherChild == nil {
			continue
		} else if child == nil {
			lvl.children[i] = otherChild
		} else {
			lvlMerger := any(child).(Merger[*Child])

			lvlMerger.MergeWith(otherChild)
		}
	}
}

func (lvl *IpLevel[Child]) CountBits() uint32 {
	var sum uint32 = 0
	for _, child := range lvl.children {
		if child == nil {
			continue
		}

		counter := any(child).(BitCounter)

		sum += uint32(counter.CountBits())
	}

	return sum
}

func (lvl *IpLevel[Child]) GetChild(part uint8) *Child {
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

func (lvl *IpLevel[Child]) Populate(wg *sync.WaitGroup) {
	for i := 0; i < 256; i++ {
		lvl.children[i] = lvl.newChild()
	}

	wg.Done()
}

func AddIp(target *FourthLevel, parts [4]uint8) bool {
	lvl3 := target.GetChild(parts[0])
	lvl2 := lvl3.GetChild(parts[1])
	lvl1 := lvl2.GetChild(parts[2])

	lastByte := parts[3]
	return lvl1.addBit(lastByte)
}
