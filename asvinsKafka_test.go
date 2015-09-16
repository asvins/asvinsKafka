package asvinsKafka

import (
	"fmt"
	"testing"
)

func TestCase1(t *testing.T) {
	Subscribe("asvins_test", func(msg string) {
		fmt.Println("Message from TEST: ", msg)
	})

	Publish("asvins_test", "Executin test 1")
	Publish("asvins_test", "Executin test 2")
	Publish("asvins_test", "Executin test 3")
	Publish("asvins_test", "Executin test 4")
	Publish("asvins_test", "Executin test 5")
	Publish("asvins_test", "Executin test 6")
	Publish("asvins_test", "Executin test 7")
}
