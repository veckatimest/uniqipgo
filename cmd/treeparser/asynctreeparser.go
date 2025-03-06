// package main

// import (
// 	"bufio"
// 	"fmt"
// 	"os"

// 	tree "github.com/Veckatimest/uniqipgo/internal/iptree"
// )

// func ReadToChan(filename string) (chan string, error) {
// 	str_chan := make(chan string, 10)
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)

// 	go func() {
// 		for scanner.Scan() {
// 			line := scanner.Text()

// 			str_chan <- line
// 		}

// 		close(str_chan)
// 	}()

// 	return str_chan, err
// }

// func CollectIpWorker(str_chan <-chan string, result chan<- *tree.FourthLevel) {
// 	workerRoot := tree.NewRoot()

// 	for line := range str_chan {
// 		octets, err := parseToOctets(line)
// 		if err != nil {
// 			// TODO: add contexts here?
// 			fmt.Printf("failed to parse ip %s", line)
// 		}

// 		tree.AddIp(workerRoot, octets)
// 	}

// 	result <- workerRoot
// }
