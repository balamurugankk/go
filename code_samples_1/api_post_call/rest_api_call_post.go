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
const urlPattern = "http://localhost:3000/users"

type Users struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	Id   int    `json:"id"`
}

type workerMessage struct {
	linesRead int64
	Id        int    `json:"id"`
	Name      string `json:"name"`
	Age       int    `json:"age"`
}

func makeRequest(workerMessage *workerMessage) {

	fullurl := urlPattern

	userRequestBody := Users{
		Id:   workerMessage.Id,
		Age:  workerMessage.Age,
		Name: workerMessage.Name,
	}
	userJsonBody, err := json.Marshal(&userRequestBody)
	httpRequest, err := http.NewRequest("POST", fullurl, bytes.NewReader(userJsonBody))
	if err != nil {
		logger.Print("httpRequest error :", err)
		return
	}

	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")

	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		logger.Printf("httpResponse error : %v", err)
		return
	}

	var body Users
	statusCode := httpResponse.StatusCode

	responseBody, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		logger.Printf("ioutil.ReadAll error %v", err)
		return
	}

	json.Unmarshal(responseBody, &body)
	if statusCode >= 200 && statusCode <= 299 {
		fmt.Printf("sno: %v,id: %v ,Name: %s, Age: %v, statusCode: %v", workerMessage.linesRead, body.Id, body.Name, body.Age, statusCode)
		fmt.Println()
	} else {

		fmt.Println("statusCode: ", statusCode)
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

		lineSplit := strings.Split(line, ",")
		id, _ := strconv.Atoi(lineSplit[0])
		name := lineSplit[1]
		age, _ := strconv.Atoi(lineSplit[2])
		workerMessage := &workerMessage{
			linesRead: linesRead,
			Id:        id,
			Name:      name,
			Age:       age,
		}
		channel <- workerMessage
		waitGroup.Add(1)

	}

	waitGroup.Wait()

	logger.Printf("linesRead = %v", linesRead)
}
