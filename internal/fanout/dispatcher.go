package fanout

import "sync"

func routedDispatcher(
	parsedBatchChan <-chan [][4]uint8,
	workerChans [](chan [][4]uint8),
	addrPool *sync.Pool,
) {
	intWc := len(workerChans)
	uint8Wc := uint8(intWc)

	parsedBatches := make([][][4]uint8, intWc)
	for i := range parsedBatches {
		parsedBatches[i] = addrPool.Get().([][4]uint8)
	}

	for addrBatch := range parsedBatchChan {
		for _, address := range addrBatch {
			idx := address[3] % uint8Wc
			parsedBatches[idx] = append(parsedBatches[idx], address)
			if len(parsedBatches[idx]) == PARSED_BATCH_SIZE {
				workerChans[idx] <- parsedBatches[idx]

				parsedBatches[idx] = addrPool.Get().([][4]uint8)
			}
		}

		addrBatch = addrBatch[:0]
		addrPool.Put(addrBatch)
	}

	for i := 0; i < intWc; i++ {
		if len(parsedBatches[i]) != 0 {
			workerChans[i] <- parsedBatches[i]
		}
	}
}
