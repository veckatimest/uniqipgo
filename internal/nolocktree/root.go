package iptree

import "github.com/Veckatimest/uniqipgo/internal/util"

// if we have total 256 buckets
// and 12 workers
// and worker #0 takes all buckets, that you can % 12 and get 0, these are 0, 12, 24, 36, etc
// then worker #3 takes all buckets, that you can % 12 and get 3, these are 3, 15, 27...
func NewRoot(workerCount int, workerIdx int) *RootLevel {
	var rootChildren [256]*ThirdLevel

	root := &RootLevel{
		newChild: FourthsChild,
		children: rootChildren,
	}

	for i := workerIdx; i < 256; i += workerCount {
		newChild := root.newChild()
		root.children[i] = newChild
		newChild.Populate()
	}

	return root
}

// AddIp returns 1 if a new bit is added and 0 if no bits was added
func AddIp(target *RootLevel, parse util.PartialParseResult) (uint32, error) {
	octetVals, err := util.ParseRest(parse.Rest)
	if err != nil {
		return 0, err
	}

	lvl3 := target.GetChild(parse.FirstByte)
	lvl2 := lvl3.GetChild(octetVals[0])
	lvl1 := lvl2.GetChild(octetVals[1])

	return lvl1.addIp(octetVals[2]), nil
}

func CountBits()
