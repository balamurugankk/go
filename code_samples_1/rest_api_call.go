package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
var waitGroup sync.WaitGroup

const numThreads = 30

type workerMessage struct {
	message string
}

func makeRequest(workerMessage string) {
	fmt.Println("aaa", workerMessage)
	return
}

func runThread(threadNumber int, channel chan *workerMessage) {
	for {
		defer waitGroup.Done()
		workerMessage := <-channel
		makeRequest(workerMessage.message)
	}

}

func main() {

	logger.Printf("len(os.Agrs)= %v", len(os.Args))

	if len(os.Args) != 2 {
		logger.Fatalf("Usage: %v <csv file path>", os.Args[0])
	}

	path := os.Args[1]

	log.Printf("Opening %v", path)

	file, err := os.Open(path)
	if err != nil {
		logger.Fatal(err)
	}
	defer file.Close()

	channel := make(chan *workerMessage, numThreads)

	log.Printf("numThreads = %v", numThreads)

	for i := 0; i < numThreads; i++ {
		go runThread(i, channel)
	}

	var linesRead int64

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		linesRead += 1
		line := scanner.Text()
		//fmt.Println(line)

		workerMessage := &workerMessage{
			message: line,
		}
		waitGroup.Add(1)
		channel <- workerMessage

	}

	go func() {
		waitGroup.Wait()
		close(channel)
	}()

	logger.Printf("linesRead = %v", linesRead)
}
