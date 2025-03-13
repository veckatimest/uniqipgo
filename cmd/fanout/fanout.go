package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	util "github.com/Veckatimest/uniqipgo/internal/util"
)

const (
	APP_NAME           = "fanout"
	RAW_BATCH_SIZE     = 6000
	PARSED_BATCH_SIZE  = 2000
	PARSER_THREADS     = 3
	DISPATCHER_THREADS = 3
	MAPPER_THREADS     = 8
	// PARSER_THREADS     = 4
	// DISPATCHER_THREADS = 2
	// MAPPER_THREADS     = 7
	BYTES_500K = 500 * 1024
)

var (
	logger           = log.Default()
	file             = flag.String("f", "", "Input file")
	profilingEnabled = flag.Bool("profile", false, "Whether to write profiling data")
)

func readToChan(filename string, strCh chan<- []string, batchPool *sync.Pool) error {
	file, err := os.Open(filename)
	if err != nil {
		close(strCh)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buffer := make([]byte, BYTES_500K)
	scanner.Buffer(buffer, BYTES_500K)
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
			// var addr [3]uint8 = ([3]uint8)(address[0:3])
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

	parsingWg.Add(PARSER_THREADS)
	for i := 0; i < PARSER_THREADS; i++ {
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
	dispatchWg.Add(DISPATCHER_THREADS)
	for i := 0; i < DISPATCHER_THREADS; i++ {
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

func run(filename string) (uint32, error) {

	workerChannels := make([](chan [][4]uint8), MAPPER_THREADS)
	for i := 0; i < MAPPER_THREADS; i++ {
		workerChannels[i] = make(chan [][4]uint8, 7)
	}

	var wg sync.WaitGroup
	var sum atomic.Uint32
	wg.Add(MAPPER_THREADS)
	addrBatchPool := sync.Pool{
		New: func() any {
			return make([][4]uint8, 0, RAW_BATCH_SIZE) // used for 2 purposes, taking larger size
		},
	}

	for i := 0; i < MAPPER_THREADS; i++ {
		go func(idx int) {
			workerChan := workerChannels[idx]
			mapCount := mapCounter(workerChan, &addrBatchPool)

			sum.Add(mapCount)
			wg.Done()
		}(i)
	}

	if readError := parallelReader(filename, workerChannels, &addrBatchPool); readError != nil {
		return 0, readError
	}

	wg.Wait()

	return sum.Load(), nil
}

func dumpMetric(idx int, metricName string) {
	metricFileName := fmt.Sprintf("profiles/%s_%s_%d.prof", APP_NAME, metricName, idx)
	metricWriter, err := os.Create(metricFileName)
	if err != nil {
		logger.Fatalf("Failed to write %s to %s", metricName, metricFileName)
	}
	defer metricWriter.Close()

	pprof.Lookup(metricName).WriteTo(metricWriter, 2)
}

func profile(profilingCtx context.Context) {
	cpu_file_name := fmt.Sprintf("%s_cpu.prof", APP_NAME)

	cpuf, err := os.Create(cpu_file_name)
	defer cpuf.Close()
	if err != nil {
		logger.Fatalf("Failed to open file %s for cpu profile: %s", cpu_file_name, err)
	}

	if err := pprof.StartCPUProfile(cpuf); err != nil {
		logger.Fatalf("Failed to write cpu profile with error %s", err)
	}
	defer pprof.StopCPUProfile()

	ticker := time.NewTicker(500 * time.Millisecond)
	count := 0
	for {
		select {
		case <-profilingCtx.Done():
			{
				return
			}
		case <-ticker.C:
			{
				dumpMetric(count, "goroutine")
				dumpMetric(count, "heap")
				count++
			}
		}
	}
}

func main() {
	flag.Parse()

	baseCtx := context.Background()

	if *profilingEnabled {
		profilingCtx, cancelFunc := context.WithCancel(baseCtx)
		go profile(profilingCtx)
		defer cancelFunc()
	}

	filename := *file
	cpuCount := runtime.NumCPU()
	logger.Printf("system has %d CPUs", cpuCount)

	start := time.Now()
	count, err := run(filename)
	if err != nil {
		logger.Fatal(err)
	}

	logger.Printf("took %v\n", time.Since(start))
	logger.Printf("Total count of unique IPs is %d\n", count)
}
