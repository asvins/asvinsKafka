# common_io

Library that wraps sarama/kafka on high-level producer and consumer for the Asvins project

# Usage
## Config/Subscribe
```go
	...
	// for each topic, there is a callback registerd to be executed when a message arrives.
	topics := make(map[string]common_io.CallbackFunc)
	// when there is a publish on the topic 'send_mail', the callback mailer.SendMail will be called
	topics["send_mail"] = mailer.SendMail

	cfg := common_io.Config{}
	// The configurations must be in a config file .gcfg
	err := config.Load("common_io_config.gcfg", &cfg)
	if err != nil {
		log.Fatal(err)
	}

	cfg.Topics = topics

	// Setup will initialize the producer and consumer
	common_io.Setup(&cfg)

	// TearDown will greacefully shutdown the consumer and producer created
	defer common_io.TearDown()
	...
```
Below is an example of a common_io_config.gcfg

	[config]
	modulename = exampleOne

	[kafka]
	brokerlist = 127.0.0.1:9092
	maxretry = 5

	[zookeeper]
	addrlist = 127.0.0.1:2181
	maxretry = 5


## Publish
```go
	...
	// Register the handler for the /api/mailTest route
	r.AddRoute("/api/mailTest", router.GET, func(w http.ResponseWriter, r *http.Request) {
		// mailer is a package that uses SMTP to send email
		// Mail is a simple struct used by this package to send formatted emails
		m := mailer.Mail{
			To:      []string{"asvins.poli@gmail.com"},
			Subject: "Test from Asvins server",
			Body:    "Test Message from Asvins Servers.\n -- Asvins Team",
		}

		b, err := json.Marshal(&m)
		if err != nil {
			fmt.Fprintf(w, err.Error())
			return
		}
		// The publish method is a wrapper for the asyncProducer of apache-kafka.
		// It will send a message(json []byte from the mailer.Mail struct) to the topic send_mail. 
		common_io.Publish("send_mail", b)
	})
	...
```
## Subscribe

### References
- http://kafka.apache.org/documentation.html
- https://github.com/Shopify/sarama
- https://github.com/wvanbergen/kafka
- https://www.youtube.com/watch?v=el-SqcZLZl
