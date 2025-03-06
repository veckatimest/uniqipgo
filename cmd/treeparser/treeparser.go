package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	tree "github.com/Veckatimest/uniqipgo/internal/iptree"
)

var (
	file = flag.String("f", "ip-list.txt", "Input file")
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

func ReadToChan(filename string) (chan string, error) {
	str_chan := make(chan string, 10)

	go func() {
		file, err := os.Open(filename)
		if err != nil {
			close(str_chan)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()

			str_chan <- line
		}
		fmt.Printf("scanner loop ended\n")

		close(str_chan)
	}()

	return str_chan, nil
}

func CollectIpWorker(str_chan <-chan string, result chan<- *tree.FourthLevel) {
	workerRoot := tree.NewRoot()

	for line := range str_chan {
		// fmt.Printf("CollectIpWorker line is read\n")
		octets, err := parseToOctets(line)
		if err != nil {
			// TODO: add contexts here?
			fmt.Printf("failed to parse ip %s", line)
		}

		tree.AddIp(workerRoot, octets)
	}

	fmt.Printf("Worker puts new root to result chan\n")
	result <- workerRoot
}

func AsyncParse(filename string) int {
	result_chan := make(chan *tree.FourthLevel)
	str_chan, err := ReadToChan(filename)

	if err != nil {
		fmt.Println("Failed to read file %s", filename)
	}

	for i := 0; i < 4; i++ {
		go CollectIpWorker(str_chan, result_chan)
	}

	mainRoot := <-result_chan
	secondRoot := <-result_chan
	mainRoot.MergeWith(secondRoot)
	for i := 0; i < 5; i++ {
		nextRoot := <-result_chan

		mainRoot.MergeWith(nextRoot)
	}
	close(result_chan)

	return mainRoot.CountBits()
}

func main() {
	flag.Parse()

	filename := *file

	start := time.Now()
	// result, err := readFileAndRun(filename)
	result := AsyncParse(filename)
	fmt.Printf("took %v\n", time.Since(start))

	// if err != nil {
	// 	fmt.Printf("Failed to handle ip list with error %s\n", err.Error())
	// }

	fmt.Printf("Total count of unique IPs is %d\n", result)
}
