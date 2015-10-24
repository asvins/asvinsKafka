package common_io

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/asvins/utils/config"
)

func TestPublishSubscribe(t *testing.T) {
	done := make(chan int)

	maxMsgs := 20
	nMsgs := 0

	topics := make(map[string]CallbackFunc)
	topics["asvins_test"] = func(msg []byte) {
		fmt.Println("Message from TEST: ", string(msg))
		nMsgs++

		if nMsgs == maxMsgs {
			fmt.Println(">> All messages were received.. DONE!")
			done <- 1
		}
	}
	c := Config{}
	err := config.Load("common_io_config.gcfg", &c)
	if err != nil {
		t.Error(err)
	}
	c.ModuleName = "testPublishSubscribe"
	c.Topics = topics

	Setup(&c)
	defer TearDown()

	for i := 0; i < maxMsgs; i++ {
		Publish("asvins_test", []byte("Execution test"+strconv.Itoa(i+1)))
	}

	// Wait until all messages are handled by the subscriber
	<-done
}
