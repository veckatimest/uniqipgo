package fanout

import (
	"bufio"
	"os"
	"sync"

	"github.com/Veckatimest/uniqipgo/internal/util"
)

func readToChan(
	filename string,
	strCh chan<- []string,
	strBatchPool *sync.Pool,
) error {
	file, err := os.Open(filename)
	if err != nil {
		close(strCh)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buffer := make([]byte, BYTES_500K)
	scanner.Buffer(buffer, BYTES_500K)
	var batch []string = strBatchPool.Get().([]string)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()

		batch = append(batch, line)
		count += 1

		if count == RAW_BATCH_SIZE {
			strCh <- batch
			count = 0

			batch = strBatchPool.Get().([]string)
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
	addrBatchChan chan<- [][4]uint8,
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

		addrBatchChan <- parsedBatch
	}

	return nil
}

func runReading(
	filename string,
	counterChans [](chan [][4]uint8),
	tc ThreadCounts,
	stringBatchPool *sync.Pool,
	addrBatchPool *sync.Pool,
) error {
	strBatchCh := make(chan []string, 10)
	parsedAddrCh := make(chan [][4]uint8, 10)
	errCh := make(chan error, 10)

	go func() {
		if err := readToChan(filename, strBatchCh, stringBatchPool); err != nil {
			errCh <- err
		}

		close(strBatchCh)
	}()

	var parsingWg sync.WaitGroup

	parsingWg.Add(tc.parserThreads)
	for i := 0; i < tc.parserThreads; i++ {
		go func() {
			err := batchParser(strBatchCh, parsedAddrCh, stringBatchPool, addrBatchPool)
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
	dispatchWg.Add(tc.dispatcherThreads)
	for i := 0; i < tc.dispatcherThreads; i++ {
		go func() {
			routedDispatcher(parsedAddrCh, counterChans, addrBatchPool)
			dispatchWg.Done()
		}()
	}

	dispatchWg.Wait()

	for _, wCh := range counterChans {
		close(wCh)
	}

	close(errCh)

	err, ok := <-errCh
	if ok {
		return err
	}
	return nil
}
