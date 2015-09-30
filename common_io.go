package common_io

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"log"
	"os"
	"os/signal"
	"time"
)

var producer sarama.AsyncProducer
var consumer *consumergroup.ConsumerGroup
var localConfig *Config

// Setup must be called in order to initialize the kafka consumer and producer
// acording to the config file
func Setup(config *Config) {
	localConfig = config
	fmt.Println(">> Initilizing Kafka for module", localConfig.ModuleName)
	initProducer()
	initConsumer()
	fmt.Println(">> Kafka initialization Done!")
}

// TearDown must be called to properly close all Kafka connections
func TearDown() {
	if err := producer.Close(); err != nil {
		fmt.Println(">> Unable to close Kafka Producer")
	}

	if consumer != nil && !consumer.Closed() {
		if err := consumer.Close(); err != nil {
			fmt.Println(">> Unable to close kafka Consumer")
		}
	}
	fmt.Println(">> TearDown execution Done!")
}

func initProducer() {
	fmt.Println(">> initProducer called")
	brokerList := localConfig.Kafka.BrokerList
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal // only wait for leader to ack
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	var err error
	producer, err = sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Faild to start KAFKA producer", err)
	}

	//You must read from the Errors() channel or the producer will deadlock.
	go func() {
		for err := range producer.Errors() {
			log.Println(">>Kadka producer Error: ", err)
		}
	}()

	fmt.Println(">> kafka producer initialized successfully")
}

func getTopicsKey() []string {
	topics := make([]string, 0, len(localConfig.Topics))
	for k := range localConfig.Topics {
		topics = append(topics, k)
	}

	return topics
}

func handleSignalInterrupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c

		TearDown()
		os.Exit(1)
	}()
}

func handleConsumerErrors() {
	go func() {
		for err := range consumer.Errors() {
			fmt.Println(err)
		}
	}()
}

func handleMessages() {
	for message := range consumer.Messages() {
		//execute the callback as as goroutine
		go localConfig.Topics[message.Topic](message.Value)

		consumer.CommitUpto(message)
	}
}

func initConsumer() {
	var err error
	fmt.Println(">> initConsumer called")
	zookeeperAddrs := localConfig.Zookeeper.AddrList
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	if topicsSize := len(localConfig.Topics); topicsSize != 0 {

		topics := getTopicsKey()

		// Creates a new consumer and adds it to the consumer group
		consumer, err = consumergroup.JoinConsumerGroup(localConfig.ModuleName, topics, zookeeperAddrs, config)

		if err != nil {
			log.Fatalln(">> Failed to start KAFKA Consumer Group\nErr:", err)
		}

		// handle signal interrupt(ctrl-c)
		handleSignalInterrupt()

		// handle consumer.Errors channel
		handleConsumerErrors()

		// start goroutine to handle incoming messages
		go handleMessages()
	}
	fmt.Println(">> kafka consumer initialized successfully")
}

// Publish .. publish a message to the kafka server using the tag and message provided.
func Publish(topic string, msg []byte) {
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}
}
