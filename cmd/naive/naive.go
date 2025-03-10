package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	naive "github.com/Veckatimest/uniqipgo/internal/naive"
)

var (
	logger          = log.Default()
	optimization    = flag.Int("o", 0, "Optimization level from 0 to 3")
	file            = flag.String("f", "ip-list.txt", "Input file")
	cpuprofile      = flag.String("cpu", "", "CPU profile target file")
	readerBatchSize = 2000
)

type IpBytes [4]uint8

func optimization0(filename string) (uint32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	stringMap := make(map[string]bool)

	var counter uint32
	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			continue
		}

		if err != nil {
			return 0, err
		}

		added, err := naive.AddStringIp(stringMap, line)
		if err != nil {
			return 0, err
		}
		counter += added
	}

	return counter, nil
}

func optimization1(filename string) (uint32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	bytesMap := naive.NewMapOfUint()

	var counter uint32
	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			continue
		}

		if err != nil {
			return 0, err
		}

		added, err := naive.AddUintIp(bytesMap, line)
		if err != nil {
			return 0, err
		}
		counter += added
	}

	return counter, nil
}

func optimization2(filename string) (uint32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	bytesMap := naive.NewMapOfBytes()

	var counter uint32
	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			continue
		}

		if err != nil {
			return 0, err
		}

		added, err := naive.AddBytesIp(bytesMap, line)
		if err != nil {
			return 0, err
		}
		counter += added
	}

	return counter, nil
}

func main() {
	flag.Parse()

	filename := *file
	cpu_file := *cpuprofile
	optimization := *optimization

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
	var err error
	var count uint32
	switch optimization {
	case 0:
		{
			logger.Println("Using map[string]bool to store ips")
			count, err = optimization0(filename)
		}
	case 1:
		{
			logger.Println("Using map[uint32]bool to store ips")
			count, err = optimization1(filename)
		}
	case 2:
		{
			logger.Println("Using map[[4]uint8]bool to store ips")
			count, err = optimization2(filename)
		}
	default:
		{
			logger.Fatal("Unsupported optimization level")
		}
	}

	logger.Printf("took %v\n", time.Since(start))

	if err != nil {
		logger.Printf("Failed to handle ip list with error %s\n", err.Error())
	}

	logger.Printf("Total count of unique IPs is %d\n", count)
}

type IpBucket struct {
	sync.Mutex
	ips   map[IpBytes]bool
	count int
}

type IpStorage struct {
	children [256]*IpBucket
}

func (st *IpStorage) AddIp(newIp IpBytes) {
	bucket := st.children[newIp[3]]

	bucket.Lock() // maybe let's use swap??
	if !bucket.ips[newIp] {
		bucket.ips[newIp] = true
		bucket.count += 1
	}
	bucket.Unlock()
}

func (st *IpStorage) CountIps() int {
	sum := 0
	for i := 0; i < 256; i++ {
		sum += st.children[i].count
	}

	return sum
}

func NewIpStorage() *IpStorage {
	var buckets [256]*IpBucket

	for i := 0; i < 256; i++ {
		buckets[i] = &IpBucket{
			ips:   make(map[IpBytes]bool),
			count: 0,
		}
	}

	return &IpStorage{
		children: buckets,
	}
}
