package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tree "github.com/Veckatimest/uniqipgo/internal/iptree"
)

var (
	logger   = log.Default()
	file     = flag.String("f", "ip-list.txt", "Input file")
	cpu_file = flag.String("cpu", "", "CPU profile file")
	async    = flag.Bool("a", false, "Use async")
)

const (
	WORKER_COUNT = 8
	BATCH_SIZE   = 1000
)

func parseToOctets(ip string) ([4]uint8, error) {
	strOctets := strings.Split(ip, ".")

	if len(strOctets) != 4 {
		return [4]uint8{}, fmt.Errorf("Invalid IP %s", ip)
	}

	result := [4]uint8{}
	for i := 0; i < 4; i++ {
		number, err := strconv.Atoi(strOctets[i])
		if err != nil {
			return [4]uint8{}, fmt.Errorf("Invalid octet %s", strOctets[i])
		}
		result[i] = uint8(number)
	}

	return result, nil
}

func readFileAndRun(filename string) (uint32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	ipTree := tree.NewRoot()
	var ipCount uint32 = 0

	for scanner.Scan() {
		line := scanner.Text()

		lineBytes, err := parseToOctets(line)
		if err != nil {
			return 0, fmt.Errorf("Failed to continue parsing IPs, bad IP %s", line)
		}

		ipCount += tree.AddIp(ipTree, lineBytes)
	}

	return ipCount, nil
}

func ReadToChan(filename string) (chan []string, error) {
	str_chan := make(chan []string, 10)

	go func() {
		file, err := os.Open(filename)
		if err != nil {
			close(str_chan)
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
				str_chan <- batch
				count = 0

				batch = make([]string, 0, BATCH_SIZE)
			}
		}

		if count != 0 {
			str_chan <- batch
		}
		fmt.Printf("scanner loop ended\n")

		close(str_chan)
	}()

	return str_chan, nil
}

func collectIpWorker(
	target *tree.FourthLevel,
	strChan <-chan []string,
	errorCh chan<- error,
) uint32 {
	var addedIps uint32
	for batch := range strChan {
		for _, line := range batch {
			octets, err := parseToOctets(line)

			if err != nil {
				// TODO: add contexts here?
				fmt.Printf("failed to parse ip %s", line)
				errorCh <- err
				return 0
			}

			added := tree.AddIp(target, octets)
			addedIps += added
		}
	}

	return addedIps
}

func asyncParse(filename string, workerCount int) (uint32, error) {
	strCh, err := ReadToChan(filename)

	if err != nil {
		fmt.Println("Failed to read file %s", filename)
		return 0, err
	}

	mainRoot := tree.NewRoot()
	var wg sync.WaitGroup
	var totalSum atomic.Uint32
	errCh := make(chan error, workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

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

	// countStart := time.Now()
	// count := totalSum.Load()
	// logger.Printf("took %v to count bits", time.Since(countStart))

	return totalSum.Load(), nil
}

func main() {
	flag.Parse()

	filename := *file
	useAsync := *async
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
	if useAsync {
		cpuCount := runtime.NumCPU()
		logger.Printf("system has %d CPUs", cpuCount)
		result, err = asyncParse(filename, cpuCount*2+1)
	} else {
		logger.Println("Using sync algorithm")
		result, err = readFileAndRun(filename)
	}
	logger.Printf("took %v\n", time.Since(start))

	if err != nil {
		fmt.Printf("Failed to handle ip list with error %s\n", err.Error())
	}

	fmt.Printf("Total count of unique IPs is %d\n", result)
}
