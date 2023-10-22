package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

var kafkaClient sarama.Client
var pom sarama.PartitionOffsetManager
var endOffsetNew, lag [40]int64
var err error
var totalLag int64 = 0
var totalMsg int64 = 0

type Configuration struct {
	GroupID    string `json:"groupId"`
	BrokerList string `json:"brokerList"`
	Topic      string `json:"topic"`
}

func main() {

	if len(os.Args) != 2 {
		log.Fatalf("usage %v <config file>", os.Args[0])
	}

	configfile := os.Args[1]
	oldOffset := make(map[int32]int64)
	newOffset := make(map[int32]int64)
	firstTime := true

	configInstance := ReadConfiguration(configfile)
	if err != nil {
		log.Fatalf("error reading configuration %v", err)
	}

	if configInstance.BrokerList == "" {
		exit(errors.New("brokers are not defined properly"))
	}

	brokers := strings.Split(configInstance.BrokerList, ",")

	kafkaClient, err = sarama.NewClient(brokers, nil)
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := sarama.NewConsumerFromClient(kafkaClient)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	partitions, err := kafkaClient.Partitions(configInstance.Topic)
	if err != nil {
		exit(err)
	}

	for {
		offsetManager, err := sarama.NewOffsetManagerFromClient(configInstance.GroupID, kafkaClient)
		if err != nil {
			exit(err)
		}
		totalLag = 0
		totalMsg = 0

		if firstTime {

			for _, partition := range partitions {

				offset, err := kafkaClient.GetOffset(configInstance.Topic, int32(partition), sarama.OffsetNewest)
				if err != nil {
					exit(err)
				}
				oldOffset[partition] = offset

			}
			firstTime = false

		} else {

			for _, partition := range partitions {
				pom, err := offsetManager.ManagePartition(configInstance.Topic, int32(partition))
				if err != nil {
					println("ManagePartion:", err)
				}
				consumerOffset, _ := pom.NextOffset()

				offset, err := kafkaClient.GetOffset(configInstance.Topic, int32(partition), sarama.OffsetNewest)
				if err != nil {
					exit(err)
				}

				endOffsetNew[partition] = offset
				lag := offset - consumerOffset
				diffMsg := offset - oldOffset[partition]
				newOffset[partition] = offset
				totalMsg = totalMsg + diffMsg
				totalLag = totalLag + lag

			}

			time.Sleep(1 * time.Second)
			fmt.Println("totalLag: ", totalLag, "totalMsg", totalMsg)

			for k, v := range newOffset {
				oldOffset[k] = v
			}

		}
	}

}

func ReadConfiguration(configfile string) *Configuration {

	log.Printf("reading config file %q", configfile)
	source, err := os.ReadFile(configfile)
	if err != nil {
		return nil
	}

	var config Configuration
	if err = json.Unmarshal(source, &config); err != nil {
		return nil
	}
	fmt.Println(config.BrokerList)
	return &config
}

func exit(err error) {
	fmt.Println(err)
	os.Exit(1)
}
