package main

import (
	"flag"
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"io"
	"strings"
)

type worker_result struct {
	max int64
	min int64
	data_points int64
	buckets map[int64]int64
}

func worker(depth int, lines chan string, bucket_size int64, results chan worker_result) {
	var result_from_children chan worker_result
	
	if depth > 0 {
		result_from_children = make(chan worker_result, 2)
		go worker(depth - 1, lines, bucket_size, result_from_children)
		go worker(depth - 1, lines, bucket_size, result_from_children)
	}

	result := do_work(lines, bucket_size)
	
	if depth > 0 {
		result = aggregate_results_from_children(result, result_from_children)
	}

	results<-result
}

func aggregate_results_from_children(result worker_result, results_from_children chan worker_result) worker_result {
	for i := 0; i < 2; i += 1 {
		b, _ := <-results_from_children
		for k,v := range b.buckets {
			result.buckets[k] += v
		}
		if b.max > result.max { result.max = b.max }
		if b.min < result.min { result.min = b.min }
		result.data_points += b.data_points
	}

	return result
}

func do_work(lines chan string, bucket_width int64) worker_result {
	buckets := make(map[int64]int64)
	max := int64(0)
	min := int64(9223372036854775807) // TODO: INTMAX
	data_points := int64(0)
	
	for {
		data, ok := <-lines
		if data == "STOP" {
			break
		}
		scanner := bufio.NewScanner(strings.NewReader(data))
		for scanner.Scan() {
			line := scanner.Text()
			
			if ok {
				i, err := strconv.ParseInt(line, 10, 64)
				if err == nil {
					bucket := i / bucket_width
					buckets[bucket] += 1
					data_points += 1

					if i > max { max = i }
					if i < min { min = i }
				} else {
					fmt.Printf("Parse error for '%s' -- ignored\n", line)
				}
			}
		}
	}

	return worker_result{max: max, min: min, data_points: data_points, buckets: buckets}
}

func main() {
	stop_signal := "STOP"
	bucket_size := flag.Int("w", 50, "The width of a bucket")
	depth := flag.Int("d", 4, "Depth of worker tree (number of workers = 2^{d+1}-1)")
	print := flag.Int("p", 10, "Number of low buckets to print")
	file_name := flag.String("f", "<no filename given>", "Name of file with data")
	flag.Parse()

	workers := (2 << *depth) - 1 // 2^{depth-1} - 1
	
	final := make(chan worker_result)
	backlog := make(chan string, workers * 2)
	
	file, err := os.Open(*file_name)

	if err != nil {
		log.Fatalf("failed to open %s\n", *file_name)
	}

	// Create worker tree in parallel
	go worker(*depth, backlog, int64(*bucket_size), final)
	// Fill work log for workers
	read_chunk(file, backlog)
	// Tell all workers to stop
	for i := 0; i < workers; i += 1 {
		backlog<-stop_signal
	}
	
	defer file.Close()

	quartiles := make(map[int64]int64)
	b, _ := <-final
	quartile_width := b.max / 4 / int64(*bucket_size) + 1
	sum := int64(0) 
	
	for k,v := range b.buckets {
		quartiles[k / quartile_width] += v
		sum += v
	}

	fmt.Println("max:    ", b.max)
	fmt.Println("min:    ", b.min)
	fmt.Println("data points:    ", b.data_points)
	fmt.Println("quartile width: ", quartile_width * int64(*bucket_size))
	for qi := 0; qi < 4; qi += 1 {
		fmt.Printf("q%v:      %v%%\n", qi + 1, float64(quartiles[int64(qi)]) / float64(sum) * 100)
	}

	fmt.Printf("---------------------------------\nBuckets 1-%v\n", *print)
	low := 0
	for bi := 0; bi < *print; bi += 1 {
		high := low + *bucket_size
		fmt.Printf("[%v,%v):\t%v\n", low, high, b.buckets[int64(bi)])
		low = high
	}

	fmt.Println("---------------------------------")
	fmt.Println("file:         ", *file_name)
	fmt.Println("bucket width: ", *bucket_size)
	fmt.Println("workers:      ", workers)
}

func read_chunk(f *os.File, sink chan string) {
	buf := make([]byte, 1024 * 1024)
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			break
		}
		s := string(buf[:n])
		overshoot := strings.LastIndex(s, "\n")
		f.Seek(int64(overshoot - n + 1), 1)
		sink<-string(buf[:overshoot])
	}
}
