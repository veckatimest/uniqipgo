package main

import (
	"flag"
	"fmt"
	rand "math/rand/v2"
	"os"
	"strconv"
	"strings"
)

var (
	count = flag.String("n", "100", "Number of ip-addresses")
	file  = flag.String("f", "ip-list.txt", "Target file")
)

func makeIp() string {
	p1 := rand.UintN(256)
	p2 := rand.UintN(256)
	p3 := rand.UintN(256)
	p4 := rand.UintN(256)

	return fmt.Sprintf("%d.%d.%d.%d\n", p1, p2, p3, p4)
}

func generateBigIpList(size int, filename string) {
	f, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)

		return
	}
	defer f.Close()

	var sb strings.Builder
	for i := 0; i < size; i++ {
		if i%1000000 == 0 {
			fmt.Printf("Writing %d\n", i)
		}

		sb.WriteString(makeIp())
		if i%10000 == 0 {
			f.WriteString(sb.String())
			sb.Reset()
		}
	}

	f.WriteString(sb.String())
}

func main() {
	flag.Parse()
	countStr := *count
	filename := *file
	countNum, err := strconv.Atoi(countStr)

	if err != nil {
		fmt.Printf("Invalid number parameter %s", countStr)
	}

	generateBigIpList(countNum, filename)
}
