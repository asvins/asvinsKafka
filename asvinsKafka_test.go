package asvinsKafka

import (
	"fmt"
	"strconv"
	"testing"
)

func TestCase1(t *testing.T) {
	Setup()
	defer TearDown()

	done := make(chan int)

	maxMsgs := 7
	nMsgs := 0

	Subscribe("asvins_test", func(msg []byte) {
		fmt.Println("Message from TEST: ", string(msg))
		nMsgs++

		if nMsgs == maxMsgs {
			fmt.Println(">> All messages were received.. DONE!")
			done <- 1
		}
	})

	for i := 0; i < maxMsgs; i++ {
		Publish("asvins_test", []byte("Execution test"+strconv.Itoa(i+1)))
	}

	// Wait until all messages are handled by the subscriber
	<-done
}
