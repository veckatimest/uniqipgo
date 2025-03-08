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
	"time"

	tree "github.com/Veckatimest/uniqipgo/internal/iptree"
)

var (
	logger   = log.Default()
	file     = flag.String("f", "ip-list.txt", "Input file")
	cpu_file = flag.String("cpu", "", "CPU profile file")
	async    = flag.Bool("a", false, "Use async")
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

func CollectIpWorker(str_chan <-chan []string, result chan<- *tree.FourthLevel, error_chan chan<- error) {
	workerRoot := tree.NewRoot()

	for batch := range str_chan {
		for _, line := range batch {
			octets, err := parseToOctets(line)

			if err != nil {
				// TODO: add contexts here?
				fmt.Printf("failed to parse ip %s", line)
				error_chan <- err
				return
			}
			tree.AddIp(workerRoot, octets)
		}

	}

	fmt.Printf("Worker puts new root to result chan\n")
	result <- workerRoot
}

func asyncParse(filename string) (uint32, error) {
	result_chan := make(chan *tree.FourthLevel)
	err_chan := make(chan error)
	str_chan, err := ReadToChan(filename)

	if err != nil {
		fmt.Println("Failed to read file %s", filename)
		return 0, err
	}

	workerCount := 4
	for i := 0; i < workerCount; i++ {
		go CollectIpWorker(str_chan, result_chan, err_chan)
	}

	results := make([]*tree.FourthLevel, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		select {
		case result := <-result_chan:
			results = append(results, result)
		case err := <-err_chan:
			return 0, err
		}
	}

	mainRoot := results[0]
	for i := 1; i < workerCount; i++ {
		nextRoot := results[i]

		mainRoot.MergeWith(nextRoot)
	}
	close(result_chan)

	bits := mainRoot.CountBits()

	return bits, nil
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
