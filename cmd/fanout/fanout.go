package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"

	fanout "github.com/Veckatimest/uniqipgo/internal/fanout"
)

const (
	APP_NAME = "fanout"
)

var (
	logger           = log.Default()
	file             = flag.String("f", "", "Input file")
	profilingEnabled = flag.Bool("profile", false, "Whether to write profiling data")
)

func dumpMetric(idx int, metricName string) {
	metricFileName := fmt.Sprintf("profiles/%s_%s_%d.prof", APP_NAME, metricName, idx)
	metricWriter, err := os.Create(metricFileName)
	if err != nil {
		logger.Fatalf("Failed to write %s to %s", metricName, metricFileName)
	}
	defer metricWriter.Close()

	pprof.Lookup(metricName).WriteTo(metricWriter, 2)
}

func profile(profilingCtx context.Context) {
	cpu_file_name := fmt.Sprintf("%s_cpu.prof", APP_NAME)

	cpuf, err := os.Create(cpu_file_name)
	defer cpuf.Close()
	if err != nil {
		logger.Fatalf("Failed to open file %s for cpu profile: %s", cpu_file_name, err)
	}

	if err := pprof.StartCPUProfile(cpuf); err != nil {
		logger.Fatalf("Failed to write cpu profile with error %s", err)
	}
	defer pprof.StopCPUProfile()

	ticker := time.NewTicker(500 * time.Millisecond)
	count := 0
	for {
		select {
		case <-profilingCtx.Done():
			{
				return
			}
		case <-ticker.C:
			{
				dumpMetric(count, "goroutine")
				dumpMetric(count, "heap")
				count++
			}
		}
	}
}

func main() {
	flag.Parse()

	baseCtx := context.Background()

	if *profilingEnabled {
		profilingCtx, cancelFunc := context.WithCancel(baseCtx)
		go profile(profilingCtx)
		defer cancelFunc()
	}

	filename := *file
	start := time.Now()
	count, err := fanout.Run(filename)
	if err != nil {
		logger.Fatal(err)
	}

	logger.Printf("took %v\n", time.Since(start))
	logger.Printf("Total count of unique IPs is %d\n", count)
}
