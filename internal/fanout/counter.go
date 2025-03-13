package fanout

import (
	"sync"
	"sync/atomic"
)

func counter(workerCh <-chan [][4]uint8, addrPool *sync.Pool) uint32 {
	store := make(map[[4]uint8]bool)
	var count uint32
	for addressBatch := range workerCh {
		for _, address := range addressBatch {
			if !store[address] {
				store[address] = true
				count++
			}
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

	for i := 0; i < tc.counterThreads; i++ {
		go func(idx int) {
			mapCount := counter(counterChans[idx], addrBatchPool)

			sum.Add(mapCount)
			wg.Done()
		}(i)
	}

	wg.Wait()

	return sum.Load(), nil
}
