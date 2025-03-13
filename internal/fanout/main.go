package fanout

import (
	"log"
	"math"
	"runtime"
	"sync"
)

var logger = log.Default()

const (
	APP_NAME          = "fanout"
	RAW_BATCH_SIZE    = 6000
	PARSED_BATCH_SIZE = 2000
	BYTES_500K        = 500 * 1024

	PARSER_THREADS     = 2
	DISPATCHER_THREADS = 1
	COUNTER_THREADS    = 4 // TODO: rename mapper to counter
	ALL_THREADS        = PARSER_THREADS + DISPATCHER_THREADS + COUNTER_THREADS
)

type ThreadCounts struct {
	parserThreads     int
	dispatcherThreads int
	counterThreads    int
}

func getThreadCount() ThreadCounts {
	numCPU := runtime.NumCPU()
	logger.Printf("System has %d CPU", numCPU)

	cpuPerThread := int(
		math.Ceil(float64(numCPU) / float64(ALL_THREADS)),
	)

	return ThreadCounts{
		parserThreads:     cpuPerThread * PARSER_THREADS,
		dispatcherThreads: cpuPerThread * DISPATCHER_THREADS,
		counterThreads:    cpuPerThread * COUNTER_THREADS,
	}
}

func Run(filename string) (uint32, error) {
	stringBatchPool := sync.Pool{
		New: func() any {
			return make([]string, 0, RAW_BATCH_SIZE)
		},
	}
	addrBatchPool := sync.Pool{
		New: func() any {
			return make([][4]uint8, 0, RAW_BATCH_SIZE) // used for 2 purposes, taking larger size
		},
	}

	tc := getThreadCount()
	counterChannels := make([](chan [][4]uint8), tc.counterThreads)
	for i := 0; i < tc.counterThreads; i++ {
		counterChannels[i] = make(chan [][4]uint8, 7)
	}

	go func() {
		if readError := runReading(
			filename,
			counterChannels,
			tc,
			&stringBatchPool,
			&addrBatchPool,
		); readError != nil {
			log.Fatal("Failure during parsing ips, exiting, %s", readError.Error())
		}
	}()

	return runCounters(counterChannels, &addrBatchPool, tc)
}
