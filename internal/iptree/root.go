package iptree

import (
	"sync"

	"github.com/Veckatimest/uniqipgo/internal/util"
)

func childMaker(idxCh <-chan int, root *RootLevel) {
	for nextIdx := range idxCh {
		newChild := root.newChild()
		root.children[nextIdx] = newChild
		newChild.Populate()
	}
}

func NewRoot(threads int) *RootLevel {
	var rootChildren [256]*ThirdLevel

	root := &RootLevel{
		newChild: FourthsChild,
		children: rootChildren,
	}

	var wg sync.WaitGroup

	wg.Add(threads)
	idxCh := make(chan int, 8)

	for i := 0; i < threads; i++ {
		go func() {
			childMaker(idxCh, root)
			wg.Done()
		}()
	}

	for i := 0; i < 256; i++ {
		idxCh <- i
	}
	close(idxCh)

	wg.Wait()

	return root
}

// AddIp returns 1 if a new bit is added and 0 if no bits was added
func AddIp(target *RootLevel, ip string) (uint32, error) {
	octetVals, err := util.ParseToOctets(ip)
	if err != nil {
		return 0, err
	}

	lvl3 := target.GetChild(octetVals[0])
	lvl2 := lvl3.GetChild(octetVals[1])
	lvl1 := lvl2.GetChild(octetVals[2])

	lastByte := octetVals[3]
	return lvl1.addIp(lastByte), nil
}
