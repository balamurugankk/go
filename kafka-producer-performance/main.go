package main

import (
	"bufio"
	"context"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/tools/tls"
)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)

var waitGroup sync.WaitGroup
var producer1 sarama.AsyncProducer
var producer2 sarama.SyncProducer
var config Configuration

const numThreads = 2

type OutKafkaConfig struct {
	Enabled           bool   `json:"enabled"`
	Brokers           string `json:"broker"`
	SecurityProtocol  string `json:"securityProtocol"`
	Topic             string `json:"topic"`
	MessageLoad       int    `json:"messageLoad"`
	TlsRootCACerts    string `json:"tlsRootCACerts"`
	TlsClientCert     string `json:"tlsClientCert"`
	TlsClientKey      string `json:"tlsClientKey"`
	Partition         int    `json:"partition"`
	Throughput        int    `json:"throughput"`
	MaxOpenRequests   int    `json:"maxOpenRequests"`
	MaxMessageBytes   int    `json:"maxMessageBytes"`
	RequiredAcks      int    `json:"requiredAcks"`
	Timeout           int    `json:"timeout"`
	Partitioner       string `json:"partitioner"`
	Compression       string `json:"compression"`
	FlushFrequency    int    `json:"flushFrequency"`
	FlushBytes        int    `json:"flushBytes"`
	FlushMessages     int    `json:"flushMessages"`
	FlushMaxMessages  int    `json:"flushMaxMessages"`
	ClientID          string `json:"clientID"`
	ChannelBufferSize int    `json:"channelBufferSize"`
	Routines          int    `json:"routines"`
	Version           string `json:"version"`
	Verbose           bool   `json:"verbose"`
}

type Configuration struct {
	OutKafkaConfig OutKafkaConfig `json:"OutKafkaConfig"`
}

type workerMessageAsync struct {
	topic       string
	partition   int
	messageLoad int
	configur    *sarama.Config
	brokers     []string
	throughPut  int
	message     string
	producer    *sarama.AsyncProducer
}

type workerMessageSync struct {
	topic       string
	partition   int
	messageLoad int
	configur    *sarama.Config
	brokers     []string
	throughPut  int
	message     string
	producer    *sarama.SyncProducer
}

// func makeRequest(workerMessage string) {
// 	fmt.Println("aaa", workerMessage)
// 	return
// }

func runAsyncProducer(topic string, partition, messageLoad int,
	config *sarama.Config, throughput int, message string, producer *sarama.AsyncProducer) {
	messages := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(partition),
		Value:     sarama.StringEncoder(string(message)),
	}
	a := *producer
	a.Input() <- messages

}

func runSyncProducer(topic string, partition, messageLoad int,
	config *sarama.Config, throughput int, message string, producer *sarama.SyncProducer) {
	messages := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(partition),
		Value:     sarama.StringEncoder(string(message)),
	}
	s := *producer
	s.SendMessage(messages)

}

func runThreadSync(threadNumber int, channel2 chan *workerMessageSync) {

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	for {
		defer waitGroup.Done()

		workerMessage2 := <-channel2
		runSyncProducer(
			workerMessage2.topic,
			workerMessage2.partition,
			workerMessage2.messageLoad,
			workerMessage2.configur,
			workerMessage2.throughPut,
			workerMessage2.message,
			workerMessage2.producer)
		cancel()
		<-done
	}

}

func runThreadAsync(threadNumber int, channel1 chan *workerMessageAsync) {

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	for {
		defer waitGroup.Done()
		workerMessage1 := <-channel1

		runAsyncProducer(workerMessage1.topic,
			workerMessage1.partition,
			workerMessage1.messageLoad,
			workerMessage1.configur,
			workerMessage1.throughPut,
			workerMessage1.message,
			workerMessage1.producer)
		cancel()
		<-done
	}

}

func parseVersion(version string) sarama.KafkaVersion {
	fmt.Println(version)
	result, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		printUsageErrorAndExit(fmt.Sprintf("unknown -version: %s", version))
	}
	return result
}

func main() {

	logger.Printf("len(os.Agrs)= %v", len(os.Args))

	if len(os.Args) != 2 {
		logger.Fatalf("Usage: %v <csv file path>", os.Args[0])
	}

	path := "data.csv"

	logger.Printf("Opening %v", path)

	source, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		logger.Fatalf("error while reading the file %v", os.Args[1])
	}

	if err = json.Unmarshal(source, &config); err != nil {
		logger.Fatalf("error while Unmarshal the file %v", err)
	}

	configur := sarama.NewConfig()

	configur.Net.MaxOpenRequests = 5
	configur.Producer.MaxMessageBytes = 100000
	configur.Producer.RequiredAcks = sarama.RequiredAcks(config.OutKafkaConfig.RequiredAcks)
	configur.Producer.Timeout = 18 * time.Second
	configur.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	configur.Producer.Compression = sarama.CompressionNone
	configur.Producer.Flush.Frequency = 0 * time.Second
	configur.Producer.Flush.Bytes = config.OutKafkaConfig.FlushBytes
	configur.Producer.Flush.Messages = config.OutKafkaConfig.FlushMessages
	configur.Producer.Flush.MaxMessages = config.OutKafkaConfig.FlushMaxMessages
	configur.Producer.Return.Successes = true
	configur.ClientID = "test-abc"
	configur.ChannelBufferSize = config.OutKafkaConfig.ChannelBufferSize

	if config.OutKafkaConfig.SecurityProtocol == "SSL" {
		tlsConfig, err := tls.NewConfig(config.OutKafkaConfig.TlsClientCert, config.OutKafkaConfig.TlsClientKey)
		if err != nil {
			printErrorAndExit(69, "failed to load client certificate from: %s and private key from: %s: %v",
				config.OutKafkaConfig.TlsClientCert, config.OutKafkaConfig.TlsClientKey, err)
		}

		if config.OutKafkaConfig.TlsRootCACerts != "" {
			rootCAsBytes, err := os.ReadFile(config.OutKafkaConfig.TlsRootCACerts)
			if err != nil {
				printErrorAndExit(69, "failed to read root CA certificates: %v", err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(rootCAsBytes) {
				printErrorAndExit(69, "failed to load root CA certificates from file: %s", config.OutKafkaConfig.TlsRootCACerts)
			}
			// Use specific root CA set vs the host's set
			tlsConfig.RootCAs = certPool
		}

		configur.Net.TLS.Enable = true
		configur.Net.TLS.Config = tlsConfig
	}

	if err := configur.Validate(); err != nil {
		printErrorAndExit(69, "Invalid configuration: %s", err)
	}

	producer1, err = sarama.NewAsyncProducer([]string{"localhost:9092"}, configur)
	if err != nil {
		printErrorAndExit(69, "Failed to create producer: %s", err)
	}

	producer2, err = sarama.NewSyncProducer([]string{"localhost:9092"}, configur)
	if err != nil {
		printErrorAndExit(69, "Failed to create producer: %s", err)
	}
	// Print out metrics periodically.

	file, err := os.Open(path)
	if err != nil {
		logger.Fatal(err)
	}
	defer file.Close()

	channel1 := make(chan *workerMessageAsync, numThreads)
	channel2 := make(chan *workerMessageSync, numThreads)
	log.Printf("numThreads = %v", numThreads)

	for i := 0; i < numThreads; i++ {
		go runThreadAsync(i, channel1)
	}

	for i := 0; i < numThreads; i++ {
		go runThreadSync(i, channel2)
	}

	var linesRead int64

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		linesRead += 1
		line := scanner.Text()
		fmt.Println(line)
		brokers := []string{"localhost:9092"}

		// workerMessage1 := &workerMessageAsync{
		// 	topic:       config.OutKafkaConfig.Topic,
		// 	partition:   config.OutKafkaConfig.Partition,
		// 	messageLoad: config.OutKafkaConfig.MessageLoad,
		// 	configur:    configur,
		// 	brokers:     brokers,
		// 	throughPut:  config.OutKafkaConfig.Throughput,
		// 	message:     line,
		// 	producer:    &producer1,
		// }
		//channel1 <- workerMessage1

		workerMessage2 := &workerMessageSync{
			topic:       config.OutKafkaConfig.Topic,
			partition:   config.OutKafkaConfig.Partition,
			messageLoad: config.OutKafkaConfig.MessageLoad,
			configur:    configur,
			brokers:     brokers,
			throughPut:  config.OutKafkaConfig.Throughput,
			message:     line,
			producer:    &producer2,
		}
		waitGroup.Add(1)
		channel2 <- workerMessage2
	}

	go func() {
		waitGroup.Wait()
		close(channel1)
	}()

	go func() {
		waitGroup.Wait()
		close(channel2)
	}()

}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}
