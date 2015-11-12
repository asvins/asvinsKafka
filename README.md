# common_io

Library that wraps sarama/kafka on high-level producer and consumer for the Asvins project

## Build status
![Build Status](https://travis-ci.org/asvins/common_io.svg)


# Usage
## Config/Consumer listen
```go
	...
	cfg := common_io.Config{}
	err := config.Load("common_io_config.gcfg", &cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Producer
	producer, err = common_io.NewProducer(cfg)
	if err != nil {
		log.Fatal(err)
	}

	defer producer.TearDown()

	// Consumer
	consumer = common_io.NewConsumer(cfg)
	consumer.HandleTopic("send_mail", mailer.SendMail)
	if err = consumer.StartListening(); err != nil {
		log.Fatal(err)
	}

	defer consumer.TearDown()
	...
```
Below is an example of a common_io_config.gcfg

	[modulename]
	value = notification

	[kafka]
	brokerlist = 127.0.0.1:9092
	maxretry = 5

	[zookeeper]
	addrlist = 127.0.0.1:2181
	maxretry = 5


## Publish
```go
	...
		m := mailer.Mail{
			To:      []string{"asvins.poli@gmail.com"},
			Subject: "Test from Asvins server",
			Body:    "Test Message from Asvins Servers.\n -- Asvins Team",
		}

		b, err := json.Marshal(&m)
		if err != nil {
			return errors.BadRequest(err.Error())
		}

		producer.Publish("send_mail", b)
	...
```
## Subscribe

### References
- http://kafka.apache.org/documentation.html
- https://github.com/Shopify/sarama
- https://github.com/wvanbergen/kafka
- https://www.youtube.com/watch?v=el-SqcZLZl
