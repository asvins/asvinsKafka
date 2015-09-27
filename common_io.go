package common_io

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

var producer sarama.AsyncProducer
var consumer sarama.Consumer

// Setup must be called in order to initialize the kafka consumer and producer
// acording to the config file
func Setup() {
	fmt.Println(">> Initilizing Kafka ")
	initProducer()
	initConsumer()
	fmt.Println(">> Done!")
}

// TearDown must be called to properly close all Kafka connections
func TearDown() {
	if err := producer.Close(); err != nil {
		fmt.Println(">> Unable to close Kafka Producer")
	}

	if err := consumer.Close(); err != nil {
		fmt.Println(">> Unbale to close Kafka Consumer")
	}
}

func initProducer() {
	brokerList := []string{"localhost:9092"}
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

func initConsumer() {
	var err error
	brokerList := []string{"localhost:9092"}
	config := sarama.NewConfig()

	consumer, err = sarama.NewConsumer(brokerList, config)

	if err != nil {
		log.Fatalln("Failed to start KAFKA consumer", err)
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

// Subscribe .. subscribe to a kafka topic.
// the callback parameter is the function to be executed for the message received.
func Subscribe(topic string, callback func(msg []byte)) {
	partitions, err := consumer.Partitions(topic)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Partitions for topic %s: are %d\n", topic, partitions)

	for _, p := range partitions {
		c, err := consumer.ConsumePartition(topic, p, sarama.OffsetNewest)

		if err != nil {
			fmt.Println(">> ERROR: Unable to initialize ConsumerPartition for topic: ", topic)
		}

		// create goroutins to stay listening to new messages as long as the process stays up
		go func() {
			for {
				select {
				case message := <-c.Messages():
					callback(message.Value)

				case err := <-c.Errors():
					fmt.Println(err)
				}
			}
		}()
	}
}
