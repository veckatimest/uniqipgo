package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	tree "github.com/Veckatimest/uniqipgo/internal/nolocktree"
	"github.com/Veckatimest/uniqipgo/internal/util"
)

type IpBytes [4]byte

// type IpsChan chan IpBytes
type IpsChan chan []util.PartialParseResult

var (
	logger   = log.Default()
	file     = flag.String("f", "ip-list.txt", "Input file")
	cpu_file = flag.String("cpu", "", "CPU profile file")
)

const (
	BATCH_SIZE = 2000
)

func collectIpWorker(
	bytesCh IpsChan,
	errCh chan<- error,
	workerCount int,
	workerIdx int,
) uint32 {
	var addedIps uint32
	root := tree.NewRoot(workerCount, workerIdx)
	for batch := range bytesCh {
		for _, parseResult := range batch {
			added, err := tree.AddIp(root, parseResult)

			if err != nil {
				errCh <- err

				return 0
			}

			addedIps += added
		}
	}

	return addedIps
}

func nolockParse(filename string, workerChans []IpsChan, errChan chan error) {
	closeAll := func() {
		for _, wc := range workerChans {
			close(wc)
		}
	}
	workerCount := len(workerChans)
	// TODO: add N buckets for channel?
	batches := make([][]util.PartialParseResult, workerCount)
	for i := 0; i < workerCount; i++ {
		batches[i] = make([]util.PartialParseResult, 0, BATCH_SIZE)
	}

	go func() {
		file, err := os.Open(filename)
		if err != nil {
			errChan <- err
			closeAll()
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parseResult, err := util.PartialParse(line)
			if err != nil {
				errChan <- err
				break
			}

			workerIdx := parseResult.FirstByte % uint8(workerCount)
			batch := batches[workerIdx]
			batch = append(batch, parseResult)
			if len(batch) == BATCH_SIZE {
				workerChans[workerIdx] <- batch
				batch = make([]util.PartialParseResult, 0, BATCH_SIZE)
			}
		}

		for idx, batch := range batches {
			if len(batch) != 0 {
				workerChans[idx] <- batch
			}
		}

		closeAll()
	}()
}

func asyncParse(filename string, workerCount int) (uint32, error) {
	workerChans := make([]IpsChan, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		workerChans = append(workerChans, make(IpsChan, 100))
	}

	errCh := make(chan error, workerCount+1)
	nolockParse(filename, workerChans, errCh)

	// mainRoot := tree.NewRoot(workerCount)
	var wg sync.WaitGroup
	var totalSum atomic.Uint32
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(idx int) {
			personalCh := workerChans[i]
			count := collectIpWorker(personalCh, errCh, workerCount, idx)
			totalSum.Add(count)
			wg.Done()
		}(i)
	}

	wg.Wait()

	close(errCh)

	haveErrors := false
	for err := range errCh {
		haveErrors = true

		logger.Printf("Error in parsing IPs %s", err.Error())
	}

	if haveErrors {
		os.Exit(1)
	}

	return totalSum.Load(), nil
}

func main() {
	flag.Parse()

	filename := *file
	cpu_file := *cpu_file

	if cpu_file != "" {
		cpuf, err := os.Create(cpu_file)
		if err != nil {
			os.Exit(1)
		}
		defer cpuf.Close()
		pprof.StartCPUProfile(cpuf)
		defer pprof.StopCPUProfile()
	}

	start := time.Now()
	var result uint32
	var err error = nil
	logger.Println("Using separate trees to concurrently add ips")
	cpuCount := runtime.NumCPU()
	logger.Printf("system has %d CPUs", cpuCount)
	result, err = asyncParse(filename, cpuCount+1)
	logger.Printf("took %v\n", time.Since(start))

	if err != nil {
		fmt.Printf("Failed to handle ip list with error %s\n", err.Error())
	}

	fmt.Printf("Total count of unique IPs is %d\n", result)
}
