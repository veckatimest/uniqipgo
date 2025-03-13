package fanout

import (
	"sync"
	"sync/atomic"

	tree "github.com/Veckatimest/uniqipgo/internal/iptree"
)

func counter(root *tree.RootLevel, workerCh <-chan [][4]uint8, addrPool *sync.Pool) uint32 {
	var count uint32
	for addressBatch := range workerCh {
		for _, address := range addressBatch {
			count += tree.AddParsedIpOptimistic(root, address)
		}
		addressBatch = addressBatch[:0]
		addrPool.Put(addressBatch)
	}

	return count
}

func runCounters(counterChans [](chan [][4]uint8), addrBatchPool *sync.Pool, tc ThreadCounts) (uint32, error) {
	var wg sync.WaitGroup
	var sum atomic.Uint32
	wg.Add(tc.counterThreads)

	tree := tree.NewRoot(tc.counterThreads)

	for i := 0; i < tc.counterThreads; i++ {
		go func(idx int) {
			mapCount := counter(tree, counterChans[idx], addrBatchPool)

			sum.Add(mapCount)
			wg.Done()
		}(i)
	}

	wg.Wait()

	return sum.Load(), nil
}
