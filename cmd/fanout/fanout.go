package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	util "github.com/Veckatimest/uniqipgo/internal/util"
)

const (
	RAW_BATCH_SIZE    = 2000
	PARSED_BATCH_SIZE = 2000
	PARSER_ROUTINES   = 3
)

var (
	logger = log.Default()
	file   = flag.String("f", "", "Input file")
)

func readToChan(filename string, strCh chan<- []string, batchPool *sync.Pool) error {
	file, err := os.Open(filename)
	if err != nil {
		close(strCh)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var batch []string = batchPool.Get().([]string)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()

		batch = append(batch, line)
		count += 1

		if count == RAW_BATCH_SIZE {
			strCh <- batch
			count = 0

			batch = batchPool.Get().([]string)
		}
	}

	if count != 0 {
		strCh <- batch
	}
	logger.Printf("scanner loop ended\n")

	return nil
}

func batchParser(
	strBatchChan <-chan []string,
	outputChan chan<- [][4]uint8,
	stringBatchPool *sync.Pool,
	addrBatchPool *sync.Pool,
) error {
	for strBatch := range strBatchChan {
		parsedBatch := addrBatchPool.Get().([][4]uint8)
		for _, line := range strBatch {
			address, err := util.ParseToOctets(line)

			if err != nil {
				return err
			}
			parsedBatch = append(parsedBatch, address)
		}
		strBatch = strBatch[:0]
		stringBatchPool.Put(strBatch)

		outputChan <- parsedBatch
	}

	return nil
}

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

func mapCounter(workerCh <-chan [][4]uint8, addrPool *sync.Pool) uint32 {
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

func parallelReader(
	filename string,
	workerChans [](chan [][4]uint8),
	addrBatchPool *sync.Pool,
) error {
	// workerCnt := len(workerChans)
	strBatchCh := make(chan []string, 10)
	parsedAddrCh := make(chan [][4]uint8, 10)
	errCh := make(chan error, 10)

	stringBatchPool := sync.Pool{
		New: func() any {
			return make([]string, 0, RAW_BATCH_SIZE)
		},
	}

	go func() {
		if err := readToChan(filename, strBatchCh, &stringBatchPool); err != nil {
			errCh <- err
		}

		close(strBatchCh)
	}()

	var parsingWg sync.WaitGroup
	parsingWg.Add(PARSER_ROUTINES)
	for i := 0; i < PARSER_ROUTINES; i++ {
		go func() {
			err := batchParser(strBatchCh, parsedAddrCh, &stringBatchPool, addrBatchPool)
			if err != nil {
				errCh <- err
			}
			parsingWg.Done()
		}()
	}

	go func() {
		parsingWg.Wait()
		close(parsedAddrCh)
	}()

	var dispatchWg sync.WaitGroup
	var dispatcherCount = 3
	dispatchWg.Add(dispatcherCount)
	for i := 0; i < dispatcherCount; i++ {
		go func() {
			routedDispatcher(parsedAddrCh, workerChans, addrBatchPool)
			dispatchWg.Done()
		}()
	}

	dispatchWg.Wait()

	for _, wCh := range workerChans {
		close(wCh)
	}

	close(errCh)

	err, ok := <-errCh
	if ok {
		return err
	}
	return nil
}

func run(filename string, workerCount int) (uint32, error) {
	workerChannels := make([](chan [][4]uint8), workerCount)
	for i := 0; i < workerCount; i++ {
		workerChannels[i] = make(chan [][4]uint8, 7)
	}

	var wg sync.WaitGroup
	var sum atomic.Uint32
	wg.Add(workerCount)

	parsedIpPool := sync.Pool{
		New: func() any {
			return make([][4]uint8, 0, PARSED_BATCH_SIZE)
		},
	}

	for i := 0; i < workerCount; i++ {
		go func(idx int) {
			workerChan := workerChannels[idx]
			mapCount := mapCounter(workerChan, &parsedIpPool)

			sum.Add(mapCount)
			wg.Done()
		}(i)
	}

	if readError := parallelReader(filename, workerChannels, &parsedIpPool); readError != nil {
		return 0, readError
	}

	wg.Wait()

	return sum.Load(), nil
}

func main() {
	flag.Parse()

	filename := *file
	logger.Printf("you chose file %s", filename)
	cpuCount := runtime.NumCPU()
	logger.Printf("system has %d CPUs", cpuCount)
	workerCount := int(float32(cpuCount) * 0.7)

	start := time.Now()
	count, err := run(filename, workerCount)
	if err != nil {
		logger.Fatal(err)
	}

	logger.Printf("took %v\n", time.Since(start))
	logger.Printf("Total count of unique IPs is %d\n", count)
}
