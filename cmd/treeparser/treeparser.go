package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
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
	workerCount = 6
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

		isNew := tree.AddIp(ipTree, lineBytes)
		if isNew {
			ipCount++
		}
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
		batch := make([]string, 0, 2000)
		count := 0
		for scanner.Scan() {
			line := scanner.Text()

			batch = append(batch, line)
			count += 1

			if count == 2000 {
				str_chan <- batch
				count = 0

				batch = make([]string, 0, 2000)
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

func CollectIpWorker(target *tree.FourthLevel, str_chan <-chan []string, error_chan chan<- error) {
	for batch := range str_chan {
		for _, line := range batch {
			octets, err := parseToOctets(line)

			if err != nil {
				// TODO: add contexts here?
				fmt.Printf("failed to parse ip %s", line)
				error_chan <- err
				return
			}

			tree.AddIp(target, octets)
		}
	}

	return
}

func asyncParse(filename string) (uint32, error) {
	str_chan, err := ReadToChan(filename)

	if err != nil {
		fmt.Println("Failed to read file %s", filename)
		return 0, err
	}

	mainRoot := tree.NewRoot()
	var wg sync.WaitGroup
	errChan := make(chan error, workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			CollectIpWorker(mainRoot, str_chan, errChan)
			wg.Done()
		}()
	}

	wg.Wait()

	close(errChan)
	haveErrors := false
	for err := range errChan {
		haveErrors = true

		logger.Printf("Error in parsing IPs %s", err.Error())
	}
	if haveErrors {
		os.Exit(1)
	}

	countStart := time.Now()
	count := mainRoot.CountBits()
	logger.Printf("took %v to count bits", time.Since(countStart))

	return count, nil
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
	// result, err := readFileAndRun(filename)
	var result uint32
	var err error = nil
	if useAsync {
		result, err = asyncParse(filename)
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
