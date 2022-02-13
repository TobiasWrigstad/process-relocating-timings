package main

import (
	"flag"
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"io"
)

func worker(depth int, lines chan string, bucket_size int64, results chan map[int64]int64) {
	var result_from_children chan map[int64]int64
	
	if depth > 0 {
		result_from_children = make(chan map[int64]int64, 2)
		go worker(depth - 1, lines, bucket_size, result_from_children)
		go worker(depth - 1, lines, bucket_size, result_from_children)
	}

	buckets := do_work(lines, bucket_size)
	
	if depth > 0 {
		aggregate_results_from_children(buckets, result_from_children)
	}

	results<-buckets
}

func aggregate_results_from_children(buckets map[int64]int64, results_from_children chan map[int64]int64) {
	for i := 0; i < 2; i += 1 {
		b, _ := <-results_from_children
		for k,v := range b {
			buckets[k] += v
		}
	}
}

func do_work(lines chan string, bucket_width int64) map[int64]int64 {
	buckets := make(map[int64]int64)
	for {
		line, ok := <-lines
		if ok {
			i, err := strconv.ParseInt(line, 10, 64)
			if err == nil {
				if i < 0 { // Channel is closed by sending -1 to all workers
					break
				}
				bucket := i / bucket_width
				buckets[bucket] += 1	
			} else {
				fmt.Printf("Parse error for '%s' -- ignored\n", line)
			}
		}
	}

	return buckets
}

func main() {
	stop_signal := "-1"
	bucket_size := flag.Int("w", 50, "The width of a bucket")
	depth := flag.Int("d", 4, "Depth of worker tree (number of workers = 2^{d+1}-1)")
	file_name := flag.String("f", "test.txt", "Name of file with data")
	flag.Parse()

	workers := (2 << *depth) - 1 // 2^{depth-1} - 1
	
	final := make(chan map[int64]int64)
	backlog := make(chan string, 128)
	
	file, err := os.Open(*file_name)

	if err != nil {
		log.Fatalf("failed to open")
	}

	// Create worker tree in parallel
	go worker(*depth, backlog, int64(*bucket_size), final)
	
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, bufio.MaxScanTokenSize)
	//scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		backlog<-scanner.Text()
	}

	for i := 0; i < workers; i += 1 {
		backlog<-stop_signal
	}
	
	file.Close()

	fmt.Println("result:", <-final)

	fmt.Println("---------------------------------")
	fmt.Println("file:         ", *file_name)
	fmt.Println("bucket width: ", *bucket_size)
	fmt.Println("workers:      ", workers)
}

func read_chunk(f os.File, sink chan string) {
	defer f.Close()
	buf := make([]byte, 1024*1024)
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			sink<-string(buf[:n])
			continue
		}
	}
}
