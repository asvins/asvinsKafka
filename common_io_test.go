package common_io

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/asvins/utils/config"
)

var c Config
var kafka_up string

func init() {
	fmt.Println("[INFO] Function init called")
	c = Config{}
	err := config.Load("common_io_config.gcfg", &c)
	if err != nil {
		log.Fatal("[ERROR] Unable to load configs: ", err)
		return
	}

	if c.ModuleName.Value == "" {
		log.Fatal("[ERROR] Module name can't be empty")
		return
	}

	kafka_up = os.Getenv("KAFKA_UP")
}

////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////// TESTS THAT DON't NEED KAFKA UP //////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
//TODO

////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////// TESTS THAT NEED KAFKA UP /////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
type nMsgs struct {
	mu    sync.Mutex
	count int
}

func newNMsgs() *nMsgs {
	return &nMsgs{sync.Mutex{}, 0}
}

func (m *nMsgs) incAndCompare(toCompare int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.count++
	if m.count == toCompare {
		done <- true
	}
}

var done chan bool = make(chan bool) // verify end of async test

func TestCreationDestruction(t *testing.T) {
	if kafka_up == "TRUE" {
		fmt.Println("-- TestCreationDestruction start --")

		p, err := NewProducer(c)
		if err != nil {
			t.Error(err)
		}

		cons := NewConsumer(c)

		cons.HandleTopic("asvins_test", func(msg []byte) {
			fmt.Println("Message from test: ", string(msg))
		})

		if err = cons.StartListening(); err != nil {
			t.Error(err)
		}

		if err = p.TearDown(); err != nil {
			t.Error(err)
		}

		if err = cons.TearDown(); err != nil {
			t.Error(err)
		}

		if !cons.consumer.Closed() {
			t.Error("[ERROR] Consumer should be closed after TearDown()")
		}

		if CreatedConsumersLength() != 0 || CreatedProducersLength() != 0 {
			t.Error("[ERROR] There should be no consumers or producers allocated")
		}

		fmt.Println("-- TestCreationDestruction end --\n")
	} else {
		fmt.Println("[INFO] -- Kafka Down.. Skipping TestCreationDestruction --\n")
	}
}

func TestPublishSubscribe(t *testing.T) {
	if kafka_up == "TRUE" {

		fmt.Println("-- TestPublishSubscribe start --")

		// Test variables
		maxMsgs := 20       // number of messages to be published
		nmsgs := newNMsgs() // counter of how many messages were received

		p, err := NewProducer(c)
		if err != nil {
			t.Error(err)
		}

		cons := NewConsumer(c)

		defer p.TearDown()
		defer cons.TearDown()

		// Add callbacks to specific topics
		cons.HandleTopic("asvins_test", func(msg []byte) {
			fmt.Println("Message from test: ", string(msg))
			nmsgs.incAndCompare(maxMsgs)
		})

		// Start listening to incoming messages
		err = cons.StartListening()
		if err != nil {
			t.Error(err)
		}

		// Publish a message
		for i := 0; i < maxMsgs; i++ {
			fmt.Println("[DEBUG] Producer Did published message with id: ", i+1)
			p.Publish("asvins_test", []byte("Execution test"+strconv.Itoa(i+1)))
		}

		// Wait until all messages are handled by the subscriber
		<-done
		<-time.After(time.Second * 2)

		if nmsgs.count > maxMsgs {
			t.Error("[ERROR] Got more messages than it should!")
		}

		fmt.Println("-- TestPublishSubscribe end --\n")
	} else {
		fmt.Println("[INFO] -- Kafka Down.. Skipping TestPublishSubscribe --\n")
	}
}

type counter struct {
	count int
	mu    sync.Mutex
}

func (cc *counter) inc() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.count++
	fmt.Println("[INFO] new count = ", cc.count)
}

func TestConsumerGroup(t *testing.T) {
	if kafka_up == "TRUE" {
		fmt.Println("-- TestConsumerGroup start --")

		fmt.Println("[INFO] If 2 c groups are created, only one of them should receive each message")

		prod, err := NewProducer(c)
		if err != nil {
			t.Error(err)
		}

		msgCount := &counter{0, sync.Mutex{}}

		consumer1 := NewConsumer(c)
		consumer2 := NewConsumer(c)

		defer prod.TearDown()
		defer consumer1.TearDown()
		defer consumer2.TearDown()

		// Add callbacks to consumer1
		consumer1.HandleTopic("asvins_test", func(msg []byte) {
			fmt.Println("Consumer1: ", string(msg))
			msgCount.inc()
		})

		//Add callbacks to consumer2
		consumer2.HandleTopic("asvins_test", func(msg []byte) {
			fmt.Println("Consumer2: ", string(msg))
			msgCount.inc()
		})

		err = consumer1.StartListening()
		if err != nil {
			t.Error(err)
		}

		err = consumer2.StartListening()
		if err != nil {
			t.Error(err)
		}

		// Publish a message
		prod.Publish("asvins_test", []byte("Execution test"))

		<-time.After(time.Second * 2)

		if msgCount.count != 1 {
			t.Error("[ERROR] Message should have been listened exactly once")
		}

		fmt.Println("-- TestConsumerGroup end --\n")
	} else {
		fmt.Println("[INFO] -- Kafka Down.. Skipping TestConsumerGroup --\n")
	}
}

func TestConsumerGroup2(t *testing.T) {
	if kafka_up == "TRUE" {
		fmt.Println("-- TestConsumerGroup2 start --")

		fmt.Println("[INFO] If 2 c groups are created, only one of them should receive each message")

		prod, err := NewProducer(c)
		if err != nil {
			t.Error(err)
		}

		msgCount := &counter{0, sync.Mutex{}}

		consumer1 := NewConsumer(c)
		c.ModuleName.Value = "another"
		consumer2 := NewConsumer(c)

		defer prod.TearDown()
		defer consumer1.TearDown()
		defer consumer2.TearDown()

		// Add callbacks to consumer1
		consumer1.HandleTopic("asvins_test2", func(msg []byte) {
			fmt.Println("Consumer1: ", string(msg))
			msgCount.inc()
		})

		//Add callbacks to consumer2
		consumer2.HandleTopic("asvins_test2", func(msg []byte) {
			fmt.Println("Consumer2: ", string(msg))
			msgCount.inc()
		})

		err = consumer1.StartListening()
		if err != nil {
			t.Error(err)
		}

		err = consumer2.StartListening()
		if err != nil {
			t.Error(err)
		}

		// Publish a message
		prod.Publish("asvins_test2", []byte("Execution test"))

		<-time.After(time.Second * 5)

		if msgCount.count != 2 {
			t.Error("[ERROR] Message should have been listened 2 times. Got", msgCount.count)
		}

		fmt.Println("-- TestConsumerGroup2 end --\n")
	} else {
		fmt.Println("[INFO] -- Kafka Down.. Skipping TestConsumerGroup --\n")
	}
}
