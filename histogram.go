package main

import (
	"flag"
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
)

// func worker(depth int, lines chan string, bucket_size int64, results chan map[int64]int64) {
// 	if depth > 0 {
// 		result_from_children := make(chan map[int64]int64, 2)
// 		go worker(depth - 1, lines, bucket_size, result_from_children)
// 		go worker(depth - 1, lines, bucket_size, result_from_children)
// 	}

// 	buckets := do_work(lines, bucket_size)
	
// 	if depth > 0 {
// 		aggregate_results_from_children(buckets, result_from_children)
// 	}

// 	results<-buckets
// }

// func aggregate_results_from_children(buckets map[int64]int64, results_from_children chan map[int64]int64) {
// 	for i := 0; i < 2; i += 1 {
// 		b, _ := <-results_from_children
// 		for k,v := range b {
// 			buckets[k] += v
// 		}
// 	}

// 	return buckets
// }

// func do_work(lines chan string, bucket_width int64) {
// 	buckets := make(map[int64]int64)
// 	for {
// 		line, ok := <-lines
// 		if ok {
// 			i, err := strconv.ParseInt(line, 10, 64)
// 			if err == nil {
// 				if i < 0 { // Channel is closed by sending -1 to all workers
// 					break
// 				}
// 				bucket := i / bucket_width
// 				buckets[bucket] += 1	
// 			} else {
// 				fmt.Printf("Parse error for '%s' -- ignored\n", line)
// 			}
// 		}
// 	}

// 	results<-buckets
// }

func process(lines chan string, bucket_size int64, results chan map[int64]int64) {
	buckets := make(map[int64]int64)
	for {
		line, ok := <-lines
		if ok {
			i, err := strconv.ParseInt(line, 10, 64)
			if err == nil {
				if i < 0 {
					break
				}
				bucket := i / bucket_size
				buckets[bucket] += 1	
			} else {
				// panic(err)
				fmt.Printf("Parse error for '%s' -- ignored\n", line)
			}
		}
	}

	results<-buckets
}

func join(buckets chan map[int64]int64, result chan map[int64]int64, joins int) {
	final_buckets := make(map[int64]int64)

	for i := 0; i < joins; i += 1 {
		b, ok := <-buckets
		if ok {
			for k,v := range b {
				final_buckets[k] += v
			}
		}
	}

	result<-final_buckets
}

func main() {
	bucket_size := flag.Int("b", 50, "The width of a bucket")
	workers := flag.Int("w", 16, "The number of workers")
	file_name := flag.String("f", "test.txt", "Name of file with data")
	flag.Parse()
	
	final := make(chan map[int64]int64)
	results := make(chan map[int64]int64, *workers)
	backlog := make(chan string, 128)
	go join(results, final, *workers)
	
	file, err := os.Open(*file_name)

	if err != nil {
		log.Fatalf("failed to open")
	}

	for i := 0; i < *workers; i += 1 {
		go process(backlog, int64(*bucket_size), results)
	}
	
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		backlog<-scanner.Text()
	}

	for i := 0; i < *workers; i += 1 {
		backlog<- "-1" // stops workers
	}
	
	file.Close()

	fmt.Println("result:", <-final)

	fmt.Println("---------------------------------")
	fmt.Println("file:         ", *file_name)
	fmt.Println("bucket width: ", *bucket_size)
	fmt.Println("workers:      ", *workers)
}
