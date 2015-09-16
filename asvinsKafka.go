package asvinsKafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

var producer sarama.SyncProducer
var consumer sarama.Consumer

// Initialize producer and consumer dts
func init() {
	fmt.Println(">> Initilizing Kafka ")
	initProducer()
	initConsumer()
	fmt.Println(">> Done!")
}

func initProducer() {
	//TODO config e broker list from config file
	brokerList := []string{"localhost:9092"}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal // only wait for leader to ack
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	var err error
	producer, err = sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Faild to start KAFKA producer", err)
	}

	fmt.Println(">> kafka producer initialized successfully")
}

func initConsumer() {
	var err error
	brokerList := []string{"localhost:9092"}
	config := sarama.NewConfig() // TODO verify correct configs

	consumer, err = sarama.NewConsumer(brokerList, config)

	if err != nil {
		log.Fatalln("Failed to start KAFKA consumer", err)
	}

	fmt.Println(">> kafka consumer initialized successfully")
}

func Publish(topic, msg string) {
	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	})

	if err != nil {
		fmt.Printf("Failed to publish message: %s, to topic: %s", msg, topic)
	}

	fmt.Printf("Message published to partition: %d, with offset: %d\n", partition, offset)
}

func Subscribe(topic string, callback func(msg string)) {
	partitions, err := consumer.Partitions(topic)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Partitions for topic %s: are %d\n", topic, partitions)

	for _, p := range partitions {
		c, err := consumer.ConsumePartition(topic, p, sarama.OffsetNewest)

		if err != nil {
			fmt.Println("DEU MERDA... TODO")
		}

		// create goroutins to stay listening to new messages as long as the process stays up
		go func() {
			for {
				select {
				case message := <-c.Messages():
					callback(string(message.Value))

				case err := <-c.Errors():
					fmt.Println(err)
				}
			}
		}()
	}
}
