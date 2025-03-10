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

	tree "github.com/Veckatimest/uniqipgo/internal/iptree"
)

var (
	logger   = log.Default()
	file     = flag.String("f", "ip-list.txt", "Input file")
	cpu_file = flag.String("cpu", "", "CPU profile file")
)

const (
	BATCH_SIZE = 2000
)

func readToChan(filename string) (chan []string, error) {
	strCh := make(chan []string, 10)

	go func() {
		file, err := os.Open(filename)
		if err != nil {
			close(strCh)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		batch := make([]string, 0, BATCH_SIZE)
		count := 0
		for scanner.Scan() {
			line := scanner.Text()

			batch = append(batch, line)
			count += 1

			if count == BATCH_SIZE {
				strCh <- batch
				count = 0

				batch = make([]string, 0, BATCH_SIZE)
			}
		}

		if count != 0 {
			strCh <- batch
		}
		fmt.Printf("scanner loop ended\n")

		close(strCh)
	}()

	return strCh, nil
}

func collectIpWorker(
	target *tree.RootLevel,
	strCh <-chan []string,
	errorCh chan<- error,
) uint32 {
	var addedIps uint32
	for batch := range strCh {
		for _, line := range batch {
			added, err := tree.AddIp(target, line)

			if err != nil {
				fmt.Printf("failed to parse ip %s", line)
				errorCh <- err
				return 0
			}

			addedIps += added
		}
	}

	return addedIps
}

type IpBytes [4]byte
type IpsChan chan IpBytes

func asyncParse(filename string, workerCount int) (uint32, error) {
	strCh, err := readToChan(filename)

	if err != nil {
		fmt.Println("Failed to read file %s", filename)
		return 0, err
	}

	mainRoot := tree.NewRoot(workerCount)
	var wg sync.WaitGroup
	var totalSum atomic.Uint32
	errCh := make(chan error, workerCount)
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			count := collectIpWorker(mainRoot, strCh, errCh)
			totalSum.Add(count)
			wg.Done()
		}()
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

	logger.Println("Using tree of trees to concurrently add ips")
	cpuCount := runtime.NumCPU()
	logger.Printf("system has %d CPUs", cpuCount)
	result, err = asyncParse(filename, cpuCount*4+1)

	logger.Printf("took %v\n", time.Since(start))

	if err != nil {
		fmt.Printf("Failed to handle ip list with error %s\n", err.Error())
	}

	fmt.Printf("Total count of unique IPs is %d\n", result)
}
