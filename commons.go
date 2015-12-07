package common_io

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

/*
*	Common events
 */
const (
	EVENT_CREATED = iota
	EVENT_UPDATED
	EVENT_DELETED
)

const DEAD_LETTER = "dead_letter"

/*
*	BuildTopicFromCommonEvent builds the topic string for the common event:
*		EVENT_CREATED, EVENT_UPDATED, EVENT_DELETED
 */
func BuildTopicFromCommonEvent(event int, prefix string) (string, error) {
	var sufix string

	switch event {
	case EVENT_CREATED:
		sufix = "_created"
	case EVENT_UPDATED:
		sufix = "_updated"
	case EVENT_DELETED:
		sufix = "_deleted"
	default:
		return "", errors.New("[ERROR] Event not found")
	}

	return prefix + sufix, nil
}

/*
*	Dead letters
 */

func doHandleDeadLetter(done chan bool, f CallbackFunc, frequency int) func(msg []byte) {
	stillWorking := make(chan bool)

	go func() {
		for {
			select {
			case <-stillWorking:
				fmt.Println("[INFO] DeadLetter Reader still has work to do")
				break

			case <-time.After(time.Minute * time.Duration(frequency)):
				fmt.Println("[INFO] DeadLetter Reader is Done")
				done <- true
				return
			}
		}
	}()

	return func(msg []byte) {
		stillWorking <- true
		f(msg)
	}
}
func HandleDeadLetter(cfg Config, topic string, f CallbackFunc) {
	flisten := func() {
		dlc := NewConsumer(cfg)

		done := make(chan bool)
		dlc.HandleTopic(topic+"_dead_letter", doHandleDeadLetter(done, f, cfg.Deadletters.Frequency/2))

		if err := dlc.StartListening(); err != nil {
			fmt.Println("[ERROR] ", err.Error())
		}
		<-done
		dlc.TearDown()
	}

	for {
		select {
		case <-time.After(time.Minute * time.Duration(cfg.Deadletters.Frequency)):
			go flisten()
			break
		}
		fmt.Println("[INFO] Number of current active goroutines: ", runtime.NumGoroutine())
	}

}
