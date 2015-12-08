package common_io

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

/*
*	The global variables are needed to execute the gracefull shutdown on SIGINT
 */
var (
	prodIndex int               = 0
	consIndex int               = 0
	producers map[int]*Producer = make(map[int]*Producer)
	consumers map[int]*Consumer = make(map[int]*Consumer)
)

func CreatedProducersLength() int {
	return len(producers)
}

func CreatedConsumersLength() int {
	return len(consumers)
}

func handleSignalInterrupt() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch

		for _, p := range producers {
			p.TearDown()
		}

		for _, c := range consumers {
			c.TearDown()
		}
		os.Exit(1)
	}()
}

func init() {
	handleSignalInterrupt()
}

/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////// PRODUCER ////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////

/*
*	Producer = saramaAsyncProducer wrapper
 */
type Producer struct {
	id       int
	producer sarama.AsyncProducer
}

/*
*	NewProducer returns a pointer newly allocated Producer object
 */
func NewProducer(config Config) (*Producer, error) {
	p, err := initProducer(&config)
	if err != nil {
		return nil, err
	} else {
		producers[prodIndex] = p
		prodIndex++
		return p, nil
	}
}

/*
*	Publish a message into a topic. Will create the topic if it doesn't exist
 */
func (p *Producer) Publish(topic string, msg []byte) {
	fmt.Println("[INFO] Publish on topic: ", topic)
	fmt.Println("[INFO] msg: ", string(msg))
	p.producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}
}

/*
*	TearDown: gracefully shut down
 */
func (p *Producer) TearDown() error {
	if p != nil {
		if err := p.producer.Close(); err != nil {
			fmt.Println("[ERROR] Unable to close Kafka Producer")
			return err
		} else {
			fmt.Println("[INFO] Producer.TearDown() done!\n")
			delete(producers, p.id)
			return nil
		}
	}
	return nil
}

func initProducer(moduleConfig *Config) (*Producer, error) {
	fmt.Println("[INFO] initProducer called")
	brokerList := moduleConfig.Kafka.BrokerList
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // only wait for leader to ack
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	var producer sarama.AsyncProducer
	var err error
	for currConnAttempt := 0; currConnAttempt < moduleConfig.Kafka.MaxRetry; currConnAttempt++ {
		producer, err = sarama.NewAsyncProducer(brokerList, config)
		if err == nil {
			break
		}
		fmt.Println("[INFO] Connection attempt faild (", (currConnAttempt + 1), "/", moduleConfig.Kafka.MaxRetry, ")")
		<-time.After(time.Second * 5)
	}

	if err != nil {
		fmt.Println("[ERROR] Unable to setup kafka producer", err)
		return nil, err
	}

	//You must read from the Errors() channel or the producer will deadlock.
	go func() {
		for err := range producer.Errors() {
			log.Println("[ERROR] Kadka producer Error: ", err)
		}
	}()

	fmt.Println("[INFO] kafka producer initialized successfully")
	return &Producer{producer: producer, id: CreatedProducersLength()}, nil
}

/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////// CONSUMER ////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////

/*
*	CallbackFunc is the function prototype for kafka callback on message received by Consumer
 */
type CallbackFunc func(msg []byte)

/*
*	Consumer = consumergroup.ConsumerGroup wrapper
 */
type Consumer struct {
	id        int
	consumer  *consumergroup.ConsumerGroup
	config    Config
	callbacks map[string]CallbackFunc
}

/*
*	NewConsumer returns a pointer newly allocated Consumer object
 */
func NewConsumer(config Config) *Consumer {
	fmt.Println("[INFO] NewConsumer() called")

	// guarantee that dead_letter frequency != 0
	if config.Deadletters.Frequency == 0 {
		config.Deadletters.Frequency = 30
	}

	c := &Consumer{consumer: nil, config: config, callbacks: make(map[string]CallbackFunc), id: CreatedConsumersLength()}
	consumers[consIndex] = c
	consIndex++
	return c
}

/*
*	HandleTopic adds a callback to the given topic and create the topic dead_letter listener
 */
func (c *Consumer) HandleTopic(topic string, callback CallbackFunc) {
	c.callbacks[topic] = callback
	//if !strings.Contains(topic, DEAD_LETTER) {
	//	go HandleDeadLetter(c.config, topic, callback)
	//}
}

/*
*	StartListening will actually create the consumer group and start listening to incoming messages
 */
func (c *Consumer) StartListening() error {
	var err error
	fmt.Println("[INFO] StartListening called")
	zookeeperAddrs := c.config.Zookeeper.AddrList
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	if topicsSize := len(c.callbacks); topicsSize != 0 {

		topics := c.getTopicsKey()

		for currConnAttempt := 0; currConnAttempt < c.config.Zookeeper.MaxRetry; currConnAttempt++ {
			// Creates a new consumer and adds it to the consumer group
			fmt.Println("[INFO] Will add consumer to group: ", c.config.ModuleName.Value)
			c.consumer, err = consumergroup.JoinConsumerGroup(c.config.ModuleName.Value, topics, zookeeperAddrs, config)
			if err == nil {
				break
			}
			fmt.Println("[INFO] Connection attempt faild (", (currConnAttempt + 1), "/", c.config.Zookeeper.MaxRetry, ")")
			<-time.After(time.Second * 5)
		}

		if err != nil {
			fmt.Println("[ERROR] Failed to start KAFKA Consumer Group\nErr:", err)
			return err
		}

		// handle consumer.Errors channel
		c.handleConsumerErrors()

		// start goroutine to handle incoming messages
		go c.handleMessages()
	}
	fmt.Println("[INFO] kafka consumer initialized successfully")
	return nil
}

/*
*	TearDown: gracefully shutdown
 */
func (c *Consumer) TearDown() error {
	if c != nil && c.consumer != nil && !c.consumer.Closed() {
		if err := c.consumer.Close(); err != nil {
			fmt.Println("[ERROR] Unable to close kafka Consumer")
			return err
		} else {
			fmt.Println("[INFO] Consumer.TearDown() done!\n")
			delete(consumers, c.id)
			return nil
		}
	}
	return nil
}

func (c *Consumer) getTopicsKey() []string {
	topics := make([]string, 0, len(c.callbacks))
	for k := range c.callbacks {
		topics = append(topics, k)
	}

	return topics
}

func (c *Consumer) handleConsumerErrors() {
	go func() {
		for err := range c.consumer.Errors() {
			fmt.Println(err)
		}
	}()
}

func (c *Consumer) handleMessages() {
	offsets := make(map[string]map[int32]int64)

	for message := range c.consumer.Messages() {
		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
			log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
		}

		fmt.Println("[INFO] Consuming from topic: ", message.Topic)
		c.callbacks[message.Topic](message.Value)

		offsets[message.Topic][message.Partition] = message.Offset
		c.consumer.CommitUpto(message)
	}
}
