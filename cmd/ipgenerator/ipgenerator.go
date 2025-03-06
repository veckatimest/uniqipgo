package main

import (
	"flag"
	"fmt"
	rand "math/rand/v2"
	"os"
	"strconv"
)

var (
	count = flag.String("n", "100", "Number of ip-addresses")
)

func makeIp() string {
	p1 := rand.UintN(256)
	p2 := rand.UintN(256)
	p3 := rand.UintN(256)
	p4 := rand.UintN(256)

	return fmt.Sprintf("%d.%d.%d.%d\n", p1, p2, p3, p4)
}

func generateBigIpList(size int) {
	f, err := os.Create("./ip-list.txt")
	if err != nil {
		fmt.Println(err)

		return
	}
	defer f.Close()

	for i := 0; i < size; i++ {
		f.WriteString(makeIp())
	}
}

func main() {
	flag.Parse()
	countStr := *count
	countNum, err := strconv.Atoi(countStr)

	if err != nil {
		fmt.Printf("Invalid number parameter %s", countStr)
	}

	generateBigIpList(countNum)
}
