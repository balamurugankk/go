package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
var waitGroup sync.WaitGroup
var httpClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   1000,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
	},
	Timeout: 60 * time.Second,
}

const numThreads = 3
const urlPattern = "http://localhost:3000/users/XXIDXX"

type Users struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	Id   int    `json:"id"`
}
type workerMessage struct {
	linesRead int64
	Id        string
}

func makeRequest(workerMessage *workerMessage) {

	fullurl := strings.ReplaceAll(urlPattern, "XXIDXX", workerMessage.Id)
	id, _ := strconv.Atoi(workerMessage.Id)
	age := id + 30
	userRequestBody := Users{
		Age: age,
		//Name: "test",
	}
	userJsonBody, err := json.Marshal(&userRequestBody)
	httpRequest, err := http.NewRequest("PUT", fullurl, bytes.NewReader(userJsonBody))
	if err != nil {
		logger.Print("err", err)
		return
	}

	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")

	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		logger.Printf("Newrequest error %v", err)
		return
	}

	var body Users
	statusCode := httpResponse.StatusCode

	mybody, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		logger.Printf("ioutil,ReadAll error %v", err)
		return
	}

	json.Unmarshal(mybody, &body)
	if statusCode == 200 {
		fmt.Printf("sno: %v,id: %v ,Name: %s, Age: %v", workerMessage.linesRead, body.Id, body.Name, body.Age)
		fmt.Println()
	}
	return
}

func runThread(threadNumber int, channel chan *workerMessage) {
	for {

		workerMessage := <-channel
		makeRequest(workerMessage)
		waitGroup.Done()
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
			linesRead: linesRead,
			Id:        line,
		}
		channel <- workerMessage
		waitGroup.Add(1)

	}

	waitGroup.Wait()

	logger.Printf("linesRead = %v", linesRead)
}
